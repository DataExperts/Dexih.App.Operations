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
using static dexih.repository.DexihDatalinkTable;
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
        public TransformWriterResult WriterResult { get; }

        private Connection TargetConnection { get; set; }
        private (Transform sourceTransform, Table SourceTable) Reader { get; set; }

        public DexihDatalink Datalink { get; set; }
        
        private DexihTable _targetTable;
        private readonly DexihHub _hub;
        private readonly SelectQuery _selectQuery;
		private readonly TransformSettings _transformSettings;
        private readonly InputColumn[] _inputColumns;

        private readonly bool _truncateTarget;
        private readonly bool _resetIncremental;
        private readonly object _resetIncrementalValue;
        private readonly GlobalVariables _globalVariables;

        private readonly ILogger _logger;


        public DatalinkRun(TransformSettings transformSettings, ILogger logger, DexihDatalink datalink, DexihHub hub, GlobalVariables globalVariables, string auditType, long referenceKey, long parentAuditKey, ETriggerMethod triggerMethod, string triggerInfo, bool truncateTarget, bool resetIncremental, object resetIncrementalValue, SelectQuery selectQuery, InputColumn[] inputColumns)
        {
            _transformSettings = transformSettings;
            _logger = logger;
            _hub = hub;
            Datalink = datalink;
            _selectQuery = selectQuery;
            _truncateTarget = truncateTarget;
            _resetIncremental = resetIncremental;
            _resetIncrementalValue = resetIncrementalValue;
            _inputColumns = inputColumns;
            _globalVariables = globalVariables;

            ReferenceKey = referenceKey;

            WriterResult = new TransformWriterResult
            {
                HubKey = hub.HubKey,
                AuditConnectionKey = datalink.AuditConnectionKey ?? 0,
                AuditType = auditType,
                ReferenceKey = referenceKey,
                ParentAuditKey = parentAuditKey,
                TriggerInfo = triggerInfo,
                TriggerMethod =  triggerMethod
            };

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
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="DatalinkRunException"></exception>
        public async Task Initialize(CancellationToken cancellationToken)
        {
            try
            {
                var timer = Stopwatch.StartNew();
                _logger.LogTrace($"Initialize datalink {Datalink.Name} started.");
                
                if(Datalink.TargetTableKey == null)
                {
                    throw new DatalinkRunException("No target table specified.");
                }
                _targetTable = _hub.GetTableFromKey((long)Datalink.TargetTableKey);
                if (_targetTable == null)
                {
                    throw new DatalinkRunException($"A target table with the key {Datalink.TargetTableKey} could not be found.");
                }

                ResetEvents();

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

                await auditConnection.InitializeAudit(WriterResult, _hub.HubKey, Datalink.AuditConnectionKey ?? 0, WriterResult.AuditType, ReferenceKey, WriterResult.ParentAuditKey, Datalink.Name, sourceKey, sourceName, Datalink.TargetTableKey ?? 0, _targetTable.Name, WriterResult.TriggerMethod, WriterResult.TriggerInfo, cancellationToken);
                WriterResult.OnProgressUpdate += Datalink_OnProgressUpdate;
                WriterResult.OnStatusUpdate += Datalink_OnStatusUpdate;
                WriterResult.TruncateTarget = _truncateTarget;
                WriterResult.ResetIncremental = _resetIncremental;
                WriterResult.ResetIncrementalValue = _resetIncrementalValue;

                //override the incremental value if it is set.
                if (_resetIncremental)
                {
                    WriterResult.LastMaxIncrementalValue = _resetIncrementalValue;
                }

                _logger.LogTrace($"Initialize datalink {Datalink.Name} audit table initialized.  Elapsed: {timer.Elapsed}.");

                var dbConnection = _hub.DexihConnections.Single(c => c.ConnectionKey == _targetTable.ConnectionKey);
                TargetConnection = dbConnection.GetConnection(_transformSettings);

                _logger.LogTrace($"Initialize datalink {Datalink.Name} completed.  Elapsed: {timer.Elapsed}.");

            }
            catch (Exception ex)
            {
                WriterResult.RunStatus = ERunStatus.Abended;
                WriterResult.Message = $"Datalink {Datalink.Name} initalize failed.  {ex.Message}";
                var newEx = new DatalinkRunException($"Datalink {Datalink.Name} initialize failed.  {ex.Message}", ex);
                throw newEx;
            }
        }

        /// <summary>
        /// Creates a plan, and compiles any scripts, in readiness to Run.
        /// </summary>
        /// <param name="inputColumns"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public void Build(CancellationToken cancellationToken)
        {
            try
            {
                var transformManager = new TransformsManager(_transformSettings, _logger);
                //Get the last Transform that will load the target table.
                Reader = transformManager.CreateRunPlan(_hub, Datalink, _inputColumns, _globalVariables, null, WriterResult.LastMaxIncrementalValue, WriterResult.TruncateTarget, _selectQuery);
            }
            catch (Exception ex)
            {
                WriterResult.RunStatus = ERunStatus.Abended;
                WriterResult.Message = $"Datalink {Datalink.Name} build failed.  {ex.Message}";
                var newEx = new DatalinkRunException($"Datalink {Datalink.Name} build failed.  {ex.Message}", ex);
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

                WriterResult.StartTime = DateTime.Now;
                WriterResult.LastUpdateTime = DateTime.Now;

                var runstatusResult =
                    await WriterResult.SetRunStatus(ERunStatus.Started, null, null, cancellationToken);
                if (!runstatusResult)
                {
                    throw new DatalinkRunException($"Failed to set run status.");
                }

                var targetTable = _targetTable.GetTable(TargetConnection, null, _transformSettings);
                var rejectTable = targetTable.GetRejectedTable(_targetTable.RejectedTableName);
                var profileTable = Reader.sourceTransform.GetProfileTable(Datalink.ProfileTableName);

                //does target table exist;
                var tableExists = await TargetConnection.TableExists(targetTable, cancellationToken);

                if (!TargetConnection.DynamicTableCreation)
                {
                    //if target table does not exist, then create it.
                    if (!tableExists)
                    {
                        await TargetConnection.CreateTable(targetTable, false, cancellationToken);
                    }
                    else
                    {
                        // compare target table to ensure all columns exist.
                        var compareTable = await TargetConnection.CompareTable(targetTable, cancellationToken);
                        if (!compareTable)
                        {
                            throw new DatalinkRunException(
                                $"Compare table {targetTable.Name} to physical table failed.");
                        }
                    }
                }

                //get the last surrogate key it there is one on the table.
                var surrogateKey = targetTable.GetDeltaColumn(TableColumn.EDeltaType.SurrogateKey);
                long surrogateKeyValue = -1;
                if (surrogateKey != null)
                {
                    surrogateKeyValue =
                        await TargetConnection.GetIncrementalKey(targetTable, surrogateKey, cancellationToken);
                }

                var targetReader = TargetConnection.GetTransformReader(targetTable);
                var transformDelta = new TransformDelta(Reader.sourceTransform, targetReader,
                    Datalink.UpdateStrategy, surrogateKeyValue, Datalink.AddDefaultRow);

                if (!await transformDelta.Open(WriterResult.AuditKey, null, cancellationToken))
                {
                    throw new DatalinkRunException("Failed to open the data reader.");
                }
                
                transformDelta.SetEncryptionMethod(EEncryptionMethod.EncryptDecryptSecureFields, _globalVariables.EncryptionKey);

                var writer = new TransformWriter();
                if (Datalink.RowsPerCommit > 0)
                {
                    writer.CommitSize = Datalink.RowsPerCommit;
                }

                var runJob = await writer.WriteAllRecords(WriterResult, transformDelta, targetTable, TargetConnection,
                    rejectTable, profileTable, cancellationToken);

                if (!runJob)
                {
                    if (WriterResult.RunStatus != ERunStatus.Abended && WriterResult.RunStatus != ERunStatus.Cancelled)
                    {
                        throw new DatalinkRunException($"Running datalink failed.");
                    }
                    return false;
                }

                if (surrogateKey != null)
                {
                    await TargetConnection.UpdateIncrementalKey(targetTable, surrogateKey.Name,
                        transformDelta.SurrogateKey, cancellationToken);
                }

                return true;
            }
            catch (OperationCanceledException)
            {
                await WriterResult.SetRunStatus(ERunStatus.Cancelled, "Datalink was cancelled", null,
                    CancellationToken.None);
                throw new DatalinkRunException($"The datalink {Datalink.Name} was cancelled.");

            }
            catch (Exception ex)
            {
                var message = $"The datalink {Datalink.Name} failed.  {ex.Message}";
                var newEx = new DatalinkRunException(message, ex);
                await WriterResult.SetRunStatus(ERunStatus.Abended, message, newEx, CancellationToken.None);
                WriterResult.RunStatus = ERunStatus.Abended;
                throw new DatalinkRunException(message, ex);
            }
            finally
            {
                OnFinish?.Invoke(WriterResult);
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
