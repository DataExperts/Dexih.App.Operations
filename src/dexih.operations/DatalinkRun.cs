using dexih.functions;
using dexih.repository;
using dexih.transforms;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
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
        
        public long DatalinkStepKey { get; set; }
        public long ParentAuditKey { get; }

        private readonly TransformWriterOptions _transformWriterOptions;
        
        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();

        private TaskCompletionSource<(DatalinkRun datalinkRun, TransformWriterResult writerResult)> _taskCompletionSource;

        public Task<(DatalinkRun datalinkRun, TransformWriterResult writerResult)> WaitForFinish()
        {
            if (_taskCompletionSource == null)
            {
                return null;
            }
            else
            {
                return _taskCompletionSource.Task;
            }
        }

        public DatalinkRun(TransformSettings transformSettings, ILogger logger, long parentAuditKey, DexihDatalink hubDatalink, DexihHub hub, InputColumn[] inputColumns, TransformWriterOptions transformWriterOptions)
        {
            ParentAuditKey = parentAuditKey;
            _transformSettings = transformSettings;
            _logger = logger;
            _hub = hub;
            Datalink = hubDatalink;
            _inputColumns = inputColumns;
            _transformWriterOptions = transformWriterOptions;
            
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

            var primaryTarget = Datalink.DexihDatalinkTargets.SingleOrDefault(c => c.IsValid && c.NodeDatalinkColumnKey == null);
            if (primaryTarget == null)
            {
                WriterTarget = new TransformWriterTarget();   
            }
            else
            {
                WriterTarget = NewTransformWriterTarget(primaryTarget, null, auditConnection);
            }

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
            var dbTargetTable = _hub.GetTableFromKey(target.TableKey);
            if (dbTargetTable == null)
            {
                throw new DatalinkRunException(
                    $"A target table with the key {target.TableKey} could not be found.");
            }

            var dbTargetConnection =
                _hub.DexihConnections.Single(c => c.IsValid && c.Key == dbTargetTable.ConnectionKey);
            var targetConnection = dbTargetConnection.GetConnection(_transformSettings);
            var targetTable = dbTargetTable.GetTable(_hub, targetConnection, _transformSettings);
            var rejectTable = dbTargetTable.GetRejectedTable(_hub, targetConnection, _transformSettings);

            var writerResult = new TransformWriterResult()
            {
                AuditConnection = auditConnection,
                AuditConnectionKey = Datalink.AuditConnectionKey ?? 0,
                AuditType = "Datalink",
                HubKey = _hub.HubKey,
                ReferenceKey = Datalink.Key,
                ParentAuditKey = ParentAuditKey,
                ReferenceName = Datalink.Name,
                SourceTableKey = Datalink.SourceDatalinkTable.SourceTable?.Key ?? 0,
                SourceTableName = Datalink.SourceDatalinkTable.Name,
                TransformWriterOptions = _transformWriterOptions,
                TargetTableKey = target.TableKey,
                TargetTableName = targetTable.Name,
                ProfileTableName = Datalink.ProfileTableName,
                RowsPerProgressEvent = Datalink.RowsPerProgress

            };

            writerResult.OnStatusUpdate += Datalink_OnStatusUpdate;
            writerResult.OnProgressUpdate += Datalink_OnProgressUpdate;

            parentWriterResult?.ChildResults.Add(writerResult);
            
            var writerTarget = new TransformWriterTarget(targetConnection, targetTable, writerResult, _transformWriterOptions, targetConnection, rejectTable, targetConnection, Datalink.ProfileTableName);
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
        public void Build(CancellationToken cancellationToken)
        {
            try
            {
                // ResetEvents();
                var transformManager = new TransformsManager(_transformSettings, _logger);
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
            Build(cancellationToken);

            void ProgressUpdate(DatalinkRun datalinkRun2, TransformWriterResult writerResult)
            {
                if (writerResult.AuditType == "Datalink")
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

        public void Schedule(DateTime startsAt, CancellationToken cancellationToken = default)
        {
            
        }

        public object Data { get => WriterTarget.WriterResult; set => throw new NotSupportedException(); }

        public void Datalink_OnProgressUpdate(TransformWriterResult writer)
        {
            OnProgressUpdate?.Invoke(this, writer);
        }

        public void Datalink_OnStatusUpdate(TransformWriterResult writer)
        {
            OnStatusUpdate?.Invoke(this, writer);
        }

        public void Dispose()
        {
            ResetEvents();
            WriterTarget?.Dispose();
        }
    }
}
