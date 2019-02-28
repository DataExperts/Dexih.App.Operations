using dexih.functions;
using dexih.functions.Query;
using dexih.repository;
using dexih.transforms;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using static dexih.transforms.Transform;
using static dexih.transforms.TransformWriterResult;

namespace dexih.operations
{
    public class DatalinkRun
    {
        #region Events
        public delegate void ProgressUpdate(TransformWriterResult writerResult);
        public delegate void StatusUpdate(TransformWriterResult writerResult);
        public delegate void DatalinkFinish(TransformWriterResult writerResult);

        public event ProgressUpdate OnProgressUpdate;
        public event StatusUpdate OnStatusUpdate;
        public event DatalinkFinish OnFinish;

        #endregion

        public long ReferenceKey; //used to track the datalink.  this can be the datalinkKey or the datalinkStep key depending on where it is run from
        
        public TransformWriterTargets WriterTargets { get; set; }
        
        private (Transform sourceTransform, Table SourceTable) Reader { get; set; }

        public DexihDatalink Datalink { get; set; }
        
        // private DexihTable _targetTable;
        private readonly DexihHub _hub;
        private readonly SelectQuery _selectQuery;
		private readonly TransformSettings _transformSettings;
        private readonly InputColumn[] _inputColumns;

        private readonly GlobalVariables _globalVariables;

        private readonly long _parentAuditKey;

        private readonly ILogger _logger;


        public DatalinkRun(TransformSettings transformSettings, ILogger logger, DexihDatalink datalink, DexihHub hub, GlobalVariables globalVariables, string auditType, long referenceKey, long parentAuditKey, ETriggerMethod triggerMethod, string triggerInfo, bool truncateTarget, bool resetIncremental, object resetIncrementalValue, SelectQuery selectQuery, InputColumn[] inputColumns)
        {
            _transformSettings = transformSettings;
            _logger = logger;
            _hub = hub;
            Datalink = datalink;
            _selectQuery = selectQuery;
            _inputColumns = inputColumns;
            _globalVariables = globalVariables;

            ReferenceKey = referenceKey;
            _parentAuditKey = parentAuditKey;
            
            var primaryWriterResult = new TransformWriterResult
            {
                HubKey = hub.HubKey,
                AuditConnectionKey = datalink.AuditConnectionKey ?? 0,
                AuditType = auditType,
                ReferenceName = datalink.Name,
                ReferenceKey = referenceKey,
                SourceTableKey = datalink.SourceDatalinkTable.SourceTable?.TableKey??0,
                SourceTableName = datalink.SourceDatalinkTable.Name,
                TargetTableKey = 0,
                TargetTableName = "",
                TriggerInfo = triggerInfo,
                TriggerMethod =  triggerMethod,
                ResetIncremental = resetIncremental,
                ResetIncrementalValue = resetIncrementalValue,
                TruncateTarget = truncateTarget
            };

            WriterTargets = new TransformWriterTargets()
            {
                WriterResult = primaryWriterResult
            };

            foreach (var target in datalink.DexihDatalinkTargets)
            {
                var writerResult = new TransformWriterResult
                {
                    HubKey = hub.HubKey,
                    AuditConnectionKey = datalink.AuditConnectionKey ?? 0,
                    AuditType = auditType,
                    ReferenceName = datalink.Name,
                    ReferenceKey = referenceKey,
                    TriggerInfo = triggerInfo,
                    TriggerMethod =  triggerMethod,
                    SourceTableKey = datalink.SourceDatalinkTable.SourceTable?.TableKey??0,
                    SourceTableName = datalink.SourceDatalinkTable.Name,
                    TargetTableKey = target.TableKey,
                    TargetTableName = target.Table?.Name,
                    ResetIncremental = resetIncremental,
                    ResetIncrementalValue = resetIncrementalValue,
                    TruncateTarget = truncateTarget
                };
                
                var dbTargetTable = _hub.GetTableFromKey(target.TableKey);
                if (dbTargetTable == null)
                {
                    throw new DatalinkRunException($"A target table with the key {target.TableKey} could not be found.");
                }
                
                var dbTargetConnection = _hub.DexihConnections.Single(c => c.ConnectionKey == dbTargetTable.ConnectionKey);
                var targetConnection = dbTargetConnection.GetConnection(_transformSettings);
                var targetTable = dbTargetTable.GetTable(targetConnection, transformSettings);

                var rejectTable = dbTargetTable.GetRejectedTable(targetConnection, transformSettings);

                var writerTarget = new TransformWriterTarget(datalink.LoadStrategy, writerResult, targetConnection, targetTable, targetConnection, rejectTable, datalink.RowsPerCommit);
                WriterTargets.Add(writerTarget);
            }
        }

        private void ResetEvents()
        {
            OnProgressUpdate = null;
            OnStatusUpdate = null;
            OnFinish = null;
        }

        /// <summary>
        /// Initializes properties required to run the datalink.  Also adds new row to the audit table.
        /// </summary>
        /// <param name="parentAuditKey"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="DatalinkRunException"></exception>
        public async Task Initialize(long parentAuditKey, CancellationToken cancellationToken)
        {
            try
            {
                var timer = Stopwatch.StartNew();
                _logger.LogTrace($"Initialize datalink {Datalink.Name} started.");
                
//                if(Datalink.TargetTableKey == null)
//                {
//                    throw new DatalinkRunException("No target table specified.");
//                }
//                _targetTable = _hub.GetTableFromKey((long)Datalink.TargetTableKey);
//                if (_targetTable == null)
//                {
//                    throw new DatalinkRunException($"A target table with the key {Datalink.TargetTableKey} could not be found.");
//                }
//                
//                var dbConnection = _hub.DexihConnections.Single(c => c.ConnectionKey == _targetTable.ConnectionKey);
//                TargetConnection = dbConnection.GetConnection(_transformSettings);


                ResetEvents();
                
                WriterTargets.OnStatusUpdate += Datalink_OnStatusUpdate;
                WriterTargets.OnProgressUpdate += Datalink_OnProgressUpdate;

                Connection auditConnection;

                if (Datalink.AuditConnectionKey > 0)
                {

                    var dbAuditConnection = _hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == Datalink.AuditConnectionKey);

                    if (dbAuditConnection == null)
                    {
                        throw new DatalinkRunException($"Audit connection with key {Datalink.AuditConnectionKey} was not found.");
                    }
                    auditConnection = dbAuditConnection.GetConnection(_transformSettings);
                }
                else
                {
                    auditConnection = new ConnectionMemory();
                }

				string sourceName = null;
				long sourceKey = 0;

                switch(Datalink.SourceDatalinkTable.SourceType)
                {
                    case ESourceType.Table:
                        if (Datalink.SourceDatalinkTable.SourceTableKey == null)
                        {
                            throw new DatalinkRunException("No source table specified.");
                        }
                        var sourceTable = _hub.GetTableFromKey(Datalink.SourceDatalinkTable.SourceTableKey.Value);
                        if (sourceTable == null)
                        {
                            throw new DatalinkRunException($"A source table with the key {Datalink.SourceDatalinkTable.SourceTableKey.Value} could not be found.");
                        }

                        sourceKey = Datalink.SourceDatalinkTable.SourceTableKey.Value;
                        sourceName = sourceTable?.Name;
                        break;
                    case ESourceType.Datalink:
                        if (Datalink.SourceDatalinkTable.SourceDatalinkKey == null)
                        {
                            throw new DatalinkRunException("No source datalink specified.");
                        }
                        var sourceDatalink = _hub.DexihDatalinks.SingleOrDefault(c => c.DatalinkKey == Datalink.SourceDatalinkTable.SourceDatalinkKey);
                        if (sourceDatalink == null)
                        {
                            throw new DatalinkRunException($"A source datalink with the key {Datalink.SourceDatalinkTable.SourceDatalinkKey.Value} could not be found.");
                        }
                        sourceKey = Datalink.SourceDatalinkTable.SourceDatalinkKey.Value;
                        sourceName = Datalink?.Name;
                        break;
                    case ESourceType.Rows:
                        sourceKey = 0;
                        sourceName = "Rows";
                        break;
                    case ESourceType.Function:
                        if (Datalink.SourceDatalinkTable.SourceTableKey == null)
                        {
                            throw new DatalinkRunException("No source function specified.");
                        }
                        var sourceFunction = _hub.GetTableFromKey(Datalink.SourceDatalinkTable.SourceTableKey.Value);
                        if (sourceFunction == null)
                        {
                            throw new DatalinkRunException($"A source function with the key {Datalink.SourceDatalinkTable.SourceTableKey.Value} could not be found.");
                        }

                        sourceKey = Datalink.SourceDatalinkTable.SourceTableKey.Value;
                        sourceName = sourceFunction?.Name;
                        break;
                }

                await WriterTargets.Initialize(parentAuditKey, auditConnection, cancellationToken);
                WriterTargets.OnProgressUpdate += Datalink_OnProgressUpdate;
                WriterTargets.OnStatusUpdate += Datalink_OnStatusUpdate;

                _logger.LogTrace($"Initialize datalink {Datalink.Name} audit table initialized.  Elapsed: {timer.Elapsed}.");

            }
            catch (Exception ex)
            {
                var message = $"Datalink {Datalink.Name} initialize failed.  {ex.Message}";
                await WriterTargets.SetRunStatus(ERunStatus.Abended, message, ex, cancellationToken);
                var newEx = new DatalinkRunException(message, ex);
                throw newEx;
            }
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
                var transformManager = new TransformsManager(_transformSettings, _logger);
                //Get the last Transform that will load the target table.
                Reader = transformManager.CreateRunPlan(_hub, Datalink, _inputColumns, _globalVariables, null, WriterTargets.WriterResult?.LastMaxIncrementalValue, WriterTargets.WriterResult?.TruncateTarget??false, _selectQuery);
            }
            catch (Exception ex)
            {
                var message = $"Datalink {Datalink.Name} build failed.  {ex.Message}";
                await WriterTargets.SetRunStatus(ERunStatus.Abended, message, ex, cancellationToken);
                var newEx = new DatalinkRunException(message, ex);
                throw newEx;
            }
        }


        /// <summary>
        /// Runs the datalink.  Note, Initialize/Build must be called prior.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="DatalinkRunException"></exception>
        public async Task<bool> Run(CancellationToken cancellationToken)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();

                await WriterTargets.SetRunStatus(ERunStatus.Started, null, null, cancellationToken);

                var sourceTransform = Reader.sourceTransform;

                switch (Datalink.UpdateStrategy)
                {
                    case TransformDelta.EUpdateStrategy.AppendUpdate:
                    case TransformDelta.EUpdateStrategy.AppendUpdateDelete:
                    case TransformDelta.EUpdateStrategy.AppendUpdatePreserve:
                    case TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve:
                        if (WriterTargets.Items.Count != 1)
                        {
                            //TODO allow multiple delta targets.
                            throw new DatalinkRunException("The delta strategies require just one target at the top node level.");
                        }

                        var target = WriterTargets.Items[0];
                        var targetReader = target.TargetConnection.GetTransformReader(target.TargetTable);
                        var transformDelta = new TransformDelta(Reader.sourceTransform, targetReader,
                            Datalink.UpdateStrategy, target.CurrentAutoIncrementKey, Datalink.AddDefaultRow);

                        if (!await transformDelta.Open(target.WriterResult?.AuditKey??0, null, cancellationToken))
                        {
                            throw new DatalinkRunException("Failed to open the data reader.");
                        }
                
                        transformDelta.SetEncryptionMethod(EEncryptionMethod.EncryptDecryptSecureFields, _globalVariables.EncryptionKey);
                        sourceTransform = transformDelta;
                        break;
                }

                var writer = new TransformWriter();
                
                var runJob = await writer.WriteRecordsAsync(sourceTransform, WriterTargets, cancellationToken);

                if (!runJob)
                {
                    if (WriterTargets.WriterResult.RunStatus != ERunStatus.Abended )
                    {
                        throw new DatalinkRunException($"Running datalink failed.");
                    }

                    if (WriterTargets.WriterResult.RunStatus != ERunStatus.Cancelled)
                    {
                        throw new DatalinkRunException($"Running datalink was cancelled.");
                    }
                    
                    return false;
                }

                return true;
            }
            catch (OperationCanceledException)
            {
                await WriterTargets.WriterResult.SetRunStatus(ERunStatus.Cancelled, "Datalink was cancelled", null,
                    CancellationToken.None);
                throw new DatalinkRunException($"The datalink {Datalink.Name} was cancelled.");

            }
            catch (Exception ex)
            {
                var message = $"The datalink {Datalink.Name} failed.  {ex.Message}";
                var newEx = new DatalinkRunException(message, ex);
                await WriterTargets.WriterResult.SetRunStatus(ERunStatus.Abended, message, newEx, CancellationToken.None);
                WriterTargets.WriterResult.RunStatus = ERunStatus.Abended;

                throw new DatalinkRunException(message, ex);
            }
            finally
            {
                OnFinish?.Invoke(WriterTargets.WriterResult);    
                foreach (var target in WriterTargets.GetAll())
                {
                    OnFinish?.Invoke(target.WriterResult);    
                }
                
            }
        }

        public void Datalink_OnProgressUpdate(TransformWriterResult writer)
        {
            OnProgressUpdate?.Invoke(writer);
        }

        public void Datalink_OnStatusUpdate(TransformWriterResult writer)
        {
            OnStatusUpdate?.Invoke(writer);
        }
    }
}
