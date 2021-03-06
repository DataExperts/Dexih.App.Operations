﻿using dexih.functions;
using dexih.repository;
using dexih.transforms;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.operations.Alerts;
using Dexih.Utils.ManagedTasks;
using Microsoft.Extensions.Logging;
using static dexih.transforms.TransformWriterResult;

namespace dexih.operations
{
    public class DatalinkRun: IManagedObject
    {
        #region Events
        public delegate void ProgressUpdate(DatalinkRun datalinkRun, TransformWriterResult writerResult);
        public delegate void StatusUpdate(DatalinkRun datalinkRun, TransformWriterResult writerResult);
        public delegate void DatalinkFinish(DatalinkRun datalinkRun, TransformWriterResult writerResult);

        public event ProgressUpdate OnProgressUpdate;
        public event StatusUpdate OnStatusUpdate;
        // public event DatalinkFinish OnFinish;

        #endregion

        public TransformWriterTarget WriterTarget { get; set; }
        
        private (Transform sourceTransform, Table SourceTable) Reader { get; set; }

        public DexihDatalink Datalink { get; set; }
        
        private readonly DexihHub _hub;
		private readonly TransformSettings _transformSettings;
        private readonly InputColumn[] _inputColumns;
        private readonly ILogger _logger;
        private readonly IAlertQueue _alertQueue;
        private readonly string[] _alertEmails;
        
        public long DatalinkStepKey { get; set; }
        public long ParentAuditKey { get; }

        private readonly TransformWriterOptions _transformWriterOptions;
        
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        private TaskCompletionSource<(DatalinkRun datalinkRun, TransformWriterResult writerResult)> _taskCompletionSource;

        public Task<(DatalinkRun datalinkRun, TransformWriterResult writerResult)> WaitForFinish()
        {
            return _taskCompletionSource?.Task;
        }

        public DatalinkRun(TransformSettings transformSettings, ILogger logger, long parentAuditKey, DexihDatalink hubDatalink, DexihHub hub, InputColumn[] inputColumns, TransformWriterOptions transformWriterOptions, IAlertQueue alertQueue, string[] alertEmails)
        {
            ParentAuditKey = parentAuditKey;
            _transformSettings = transformSettings;
            _logger = logger;
            _hub = hub;
            Datalink = hubDatalink;
            _inputColumns = inputColumns;
            _transformWriterOptions = transformWriterOptions;
            _alertQueue = alertQueue;
            _alertEmails = alertEmails;
            
            Connection auditConnection;

            if (Datalink.AuditConnectionKey > 0)
            {
                var dbAuditConnection =
                    _hub.DexihConnections.SingleOrDefault(c => c.IsValid &&  c.Key == Datalink.AuditConnectionKey);

                if (dbAuditConnection == null)
                {
                    throw new DatalinkRunException(
                        $"Audit connection with key {Datalink.AuditConnectionKey} was not found.");
                }

                auditConnection = dbAuditConnection.GetConnection(_transformSettings);
            }
            else
            {
                auditConnection = null;
            }

            var datalinkTable = Datalink.GetOutputTable();

            // get the top level target
            if (Datalink.DexihDatalinkTargets.Count(c => c.IsValid &&  c.NodeDatalinkColumnKey == null) > 1)
            {
                throw new DatalinkRunException("There are multiple targets set for the top level.  Remove the extra targets, or set the nodeLevel.");
            }

            // primaryTarget can be null, this is handled in the NewTransformWriterTarget function
            var primaryTarget = Datalink.DexihDatalinkTargets.SingleOrDefault(c => c.IsValid && c.NodeDatalinkColumnKey == null);
            WriterTarget = NewTransformWriterTarget(primaryTarget, null, auditConnection);

            foreach (var target in Datalink.DexihDatalinkTargets.Where(c=> c.IsValid && c.NodeDatalinkColumnKey != null))
            {
                string[] nodePath = null;
                if (target.NodeDatalinkColumnKey != null)
                {
                    var nodeColumns = datalinkTable.GetNodePath(target.NodeDatalinkColumnKey.Value, null);
                    nodePath = nodeColumns.Select(c => c.Name).ToArray();
                }

                var childWriterTarget = NewTransformWriterTarget(target, WriterTarget.WriterResult, auditConnection);
                WriterTarget.Add(childWriterTarget, nodePath);
            }
        }

        private TransformWriterTarget NewTransformWriterTarget(DexihDatalinkTarget target, TransformWriterResult parentWriterResult, Connection auditConnection)
        {
            Table targetTable = null;
            Table rejectTable = null;
            Connection targetConnection = null;
            if (target != null)
            {
                var dbTargetTable = _hub.GetTableFromKey(target.TableKey);
                var dbTargetConnection =
                    _hub.DexihConnections.Single(c => c.IsValid && c.Key == dbTargetTable.ConnectionKey);
                targetConnection = dbTargetConnection.GetConnection(_transformSettings);
                targetTable = dbTargetTable.GetTable(_hub, targetConnection, _transformSettings);
                rejectTable = dbTargetTable.GetRejectedTable(_hub, targetConnection, _transformSettings);
            }

            var writerResult = new TransformWriterResult()
            {
                AuditConnection = auditConnection,
                AuditConnectionKey = Datalink.AuditConnectionKey ?? 0,
                AuditType = Constants.Datalink,
                HubKey = _hub.HubKey,
                ReferenceKey = Datalink.Key,
                ParentAuditKey = ParentAuditKey,
                ReferenceName = Datalink.Name,
                SourceTableKey = Datalink.SourceDatalinkTable.SourceTable?.Key ?? 0,
                SourceTableName = Datalink.SourceDatalinkTable.Name,
                TransformWriterOptions = _transformWriterOptions,
                TargetTableKey = target?.TableKey??0,
                TargetTableName = targetTable?.Name??"No target table",
                ProfileTableName = Datalink.ProfileTableName,
                RowsPerProgressEvent = Datalink.RowsPerProgress

            };

            writerResult.OnStatusUpdate += Datalink_OnStatusUpdate;
            writerResult.OnProgressUpdate += Datalink_OnProgressUpdate;

            parentWriterResult?.ChildResults.Add(writerResult);

            var writerTarget = new TransformWriterTarget(targetConnection, targetTable, writerResult, _transformWriterOptions, targetConnection, rejectTable, auditConnection, Datalink.ProfileTableName) 
            {
                AddDefaultRow = Datalink.AddDefaultRow
            };
            return writerTarget;
        }

        private void ResetEvents()
        {
            OnProgressUpdate = null;
            OnStatusUpdate = null;
            // OnFinish = null;
        }

        /// <summary>
        /// Creates a plan, and compiles any scripts, in readiness to Run.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task Build(CancellationToken cancellationToken)
        {
            try
            {
                // ResetEvents();
                var transformManager = new TransformsManager(_transformSettings, _logger);
                if(WriterTarget.WriterResult != null)
                {
                    await WriterTarget.WriterResult.Initialize(cancellationToken);
                }

                Reader = transformManager.CreateRunPlan(_hub, Datalink, _inputColumns, null, WriterTarget.WriterResult?.LastMaxIncrementalValue, _transformWriterOptions);
            }
            catch (Exception ex)
            {
                var message = $"Datalink {Datalink.Name} build failed.  {ex.Message}";
                WriterTarget.SetRunStatus(ERunStatus.Abended, message, ex, cancellationToken);
                var newEx = new DatalinkRunException(message, ex);
                throw newEx;
            }
        }

        /// <summary>
        /// Sets the datalink in activation mode, which means the "waitforfinish" won't complete until datalink is finished.
        /// </summary>
        public void ActivateDatalink()
        {
            _taskCompletionSource = new TaskCompletionSource<(DatalinkRun datalinkRun, TransformWriterResult writerResult)>();
        }

        /// <summary>
        /// Runs the datalink.  Note, Initialize/Build must be called prior.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="DatalinkRunException"></exception>
        public async Task Run(CancellationToken cancellationToken)
        {
            try
            {
                var ct = CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token,
                    cancellationToken);

                var token = ct.Token;
                token.ThrowIfCancellationRequested();

                if (_taskCompletionSource == null)
                {
                    _taskCompletionSource = new TaskCompletionSource<(DatalinkRun datalinkRun, TransformWriterResult writerResult)>();
                }

                await WriterTarget.WriteRecordsAsync(Reader.sourceTransform, Datalink.UpdateStrategy,
                    Datalink.LoadStrategy, token);

                await Reader.sourceTransform.DisposeAsync();
            }
            finally
            {
                var taskCompletion = _taskCompletionSource;
                _taskCompletionSource = null;
                taskCompletion?.SetResult((this, WriterTarget.WriterResult));
            }
        }

        public async Task StartAsync(ManagedTaskProgress progress, CancellationToken cancellationToken = default)
        {
            progress.Report(0, 0, "Compiling datalink...");
            await Build(cancellationToken);

            void ProgressUpdate(DatalinkRun datalinkRun2, TransformWriterResult writerResult)
            {
                if (writerResult.AuditType == Constants.Datalink)
                {
                    progress.Report(writerResult.PercentageComplete,
                        writerResult.RowsTotal + writerResult.RowsReadPrimary,
                        writerResult.IsFinished ? "" : "Running datalink...");
                }
            }

            OnProgressUpdate += ProgressUpdate;
            OnStatusUpdate += ProgressUpdate;

            progress.Report(0, 0, "Running datalink...");
            await Run(cancellationToken);
        }

        public void Cancel()
        {
            _cancellationTokenSource.Cancel();
        }

        public void Schedule(DateTimeOffset startsAt, CancellationToken cancellationToken = default)
        {
            
        }

        public object Data { get => WriterTarget.WriterResult; set => throw new NotSupportedException(); }

        public void Datalink_OnProgressUpdate(TransformWriterResult writer)
        {
            OnProgressUpdate?.Invoke(this, writer);
        }

        public void Datalink_OnStatusUpdate(TransformWriterResult writer)
        {
            if (_alertQueue != null && Datalink.AlertLevel != EAlertLevel.None)
            {
                switch (writer.RunStatus)
                {
                    case ERunStatus.Started:
                        if (Datalink.AlertLevel == EAlertLevel.All)
                        {
                            _alertQueue.Add(new Alert()
                            {
                                Emails = _alertEmails,
                                Subject = $"Datalink {Datalink.Name} has started.",
                                Body = $"The datalink {Datalink.Name} started at {DateTime.Now}"
                            });
                        }

                        break;
                    case ERunStatus.Finished:
                        if (Datalink.AlertLevel == EAlertLevel.All)
                        {
                            _alertQueue.Add(new Alert()
                            {
                                Emails = _alertEmails,
                                Subject = $"Datalink {Datalink.Name} finished successfully.",
                                Body = $"The datalink {Datalink.Name} finished successfully at {DateTime.Now}"
                            });
                        }

                        break;
                    case ERunStatus.FinishedErrors:
                        if (Datalink.AlertLevel == EAlertLevel.Errors || Datalink.AlertLevel == EAlertLevel.Critical)
                        {
                            _alertQueue.Add(new Alert()
                            {
                                Emails = _alertEmails,
                                Subject = $"Datalink {Datalink.Name} finished with some errors.",
                                Body =
                                    $"The datalink {Datalink.Name} finished with some errors at {DateTime.Now}.\n\n{writer.Message}"
                            });
                        }

                        break;
                    case ERunStatus.Abended:
                    case ERunStatus.Cancelled:
                    case ERunStatus.Failed:
                        if (Datalink.AlertLevel != EAlertLevel.None)
                        {
                            _alertQueue.Add(new Alert()
                            {
                                Emails = _alertEmails,
                                Subject = $"Datalink {Datalink.Name} finished with status {writer.RunStatus}.",
                                Body =
                                    $"The datalink {Datalink.Name} finished with status {writer.RunStatus} at {DateTime.Now}.\n\n{writer.Message}"
                            });
                        }

                        break;
                }
            }

            OnStatusUpdate?.Invoke(this, writer);
        }

        public void Dispose()
        {
            ResetEvents();
            WriterTarget?.Dispose();
        }
    }
}
