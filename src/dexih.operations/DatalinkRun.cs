using dexih.functions;
using dexih.repository;
using dexih.transforms;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using static dexih.transforms.TransformWriterResult;

namespace dexih.operations
{
    public class DatalinkRun
    {
        #region Events
        public delegate void ProgressUpdate(DatalinkRun datalinkRun, TransformWriterResult writerResult);
        public delegate void StatusUpdate(DatalinkRun datalinkRun, TransformWriterResult writerResult);
        public delegate void DatalinkFinish(DatalinkRun datalinkRun, TransformWriterResult writerResult);

        public event ProgressUpdate OnProgressUpdate;
        public event StatusUpdate OnStatusUpdate;
        public event DatalinkFinish OnFinish;

        #endregion

        public TransformWriterTarget WriterTarget { get; set; }
        
        private (Transform sourceTransform, Table SourceTable) Reader { get; set; }

        public DexihDatalink Datalink { get; set; }
        
        
        // private DexihTable _targetTable;
        private readonly DexihHub _hub;
		private readonly TransformSettings _transformSettings;
        private readonly InputColumn[] _inputColumns;
        private readonly ILogger _logger;
        
        public long DatalinkStepKey { get; set; }
        public long ParentAuditKey { get; }

        private readonly TransformWriterOptions _transformWriterOptions;


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
                    _hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == Datalink.AuditConnectionKey);

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
            var primaryTarget = Datalink.DexihDatalinkTargets.SingleOrDefault(c => c.NodeDatalinkColumnKey == null);
            if (primaryTarget == null)
            {
                WriterTarget = new TransformWriterTarget();   
            }
            else
            {
                WriterTarget = NewTransformWriterTarget(primaryTarget, null, auditConnection);
            }

            foreach (var target in Datalink.DexihDatalinkTargets.Where(c=>c.NodeDatalinkColumnKey != null))
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
                _hub.DexihConnections.Single(c => c.ConnectionKey == dbTargetTable.ConnectionKey);
            var targetConnection = dbTargetConnection.GetConnection(_transformSettings);
            var targetTable = dbTargetTable.GetTable(_hub, targetConnection, _transformSettings);
            var rejectTable = dbTargetTable.GetRejectedTable(_hub, targetConnection, _transformSettings);

            var writerResult = new TransformWriterResult()
            {
                AuditConnection = auditConnection,
                AuditConnectionKey = Datalink.AuditConnectionKey ?? 0,
                AuditType = "Datalink",
                HubKey = _hub.HubKey,
                ReferenceKey = Datalink.DatalinkKey,
                ParentAuditKey = ParentAuditKey,
                ReferenceName = Datalink.Name,
                SourceTableKey = Datalink.SourceDatalinkTable.SourceTable?.TableKey ?? 0,
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
            OnFinish = null;
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
                ResetEvents();

                var transformManager = new TransformsManager(_transformSettings, _logger);
                //Get the last Transform that will load the target table.
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
        /// Runs the datalink.  Note, Initialize/Build must be called prior.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="DatalinkRunException"></exception>
        public Task Run(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return WriterTarget.WriteRecordsAsync(Reader.sourceTransform, Datalink.UpdateStrategy, Datalink.LoadStrategy, cancellationToken);
        }

        public void Datalink_OnProgressUpdate(TransformWriterResult writer)
        {
            OnProgressUpdate?.Invoke(this, writer);
        }

        public void Datalink_OnStatusUpdate(TransformWriterResult writer)
        {
            OnStatusUpdate?.Invoke(this, writer);
        }
    }
}
