using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.repository;
using dexih.transforms;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;
using Dexih.Utils.ManagedTasks;
using Microsoft.Extensions.Logging;

namespace dexih.operations
{
    public enum EStartMode { RunSnapshot = 1, RunTests}
    public class DatalinkTestRun: IManagedObject
    {        
        
        const int MaxErrorRows = 100;
            
        #region Events
        public delegate void ProgressUpdate(TransformWriterResult writerResult);
        public delegate void StatusUpdate(TransformWriterResult writerResult);
        public delegate void DatalinkTestFinish(TransformWriterResult writerResult);

        public event ProgressUpdate OnProgressUpdate;
        public event StatusUpdate OnStatusUpdate;

        #endregion
        
        public TransformWriterResult WriterResult { get; set; }
        
        private readonly DexihDatalinkTest _datalinkTest;
        private readonly DexihHub _hub;
        private readonly TransformSettings _transformSettings;
        private readonly TransformWriterOptions _transformWriterOptions;

        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        
        public List<TestResult> TestResults;

        private readonly ILogger _logger;

        //determines whether the "Start" action runs the tests or a snapshot.
        public EStartMode StartMode = EStartMode.RunTests;

        public DatalinkTestRun(
            TransformSettings transformSettings,
            ILogger logger, 
            DexihDatalinkTest datalinkTest, 
            DexihHub hub, 
            TransformWriterOptions transformWriterOptions
            )
        {
            _transformSettings = transformSettings;
            _transformWriterOptions = transformWriterOptions;
            _logger = logger;
            
            // create a copy of the hub as the test run will update objects.
            _hub = hub.CloneProperties<DexihHub>();
            
            _datalinkTest = datalinkTest;
            
            Connection auditConnection;
            
            if (datalinkTest.AuditConnectionKey > 0)
            {
                var dbAuditConnection =
                    _hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == datalinkTest.AuditConnectionKey);

                if (dbAuditConnection == null)
                {
                    throw new DatalinkRunException(
                        $"Audit connection with key {datalinkTest.AuditConnectionKey} was not found.");
                }

                auditConnection = dbAuditConnection.GetConnection(_transformSettings);
            }
            else
            {
                auditConnection = new ConnectionMemory();
            }
            
            TestResults = new List<TestResult>();

            WriterResult = new TransformWriterResult()
            {
                AuditConnection = auditConnection,
                AuditConnectionKey = datalinkTest.AuditConnectionKey ?? 0,
                AuditType = "DatalinkTest",
                HubKey = _hub.HubKey,
                ReferenceKey = datalinkTest.Key,
                ParentAuditKey = 0,
                ReferenceName = datalinkTest.Name,
                SourceTableKey = 0,
                SourceTableName = "",
                TransformWriterOptions = _transformWriterOptions
            };
        }

        private void UpdateProgress(int percent, string message = null, Exception exception = null)
        {
            WriterResult.RowsCreated = percent;
            WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Running, message, exception);
        }
        
        public void DatalinkTest_OnProgressUpdate(TransformWriterResult writer)
        {
            OnProgressUpdate?.Invoke(writer);
        }

        public void DatalinkTest_OnStatusUpdate(TransformWriterResult writer)
        {
            OnStatusUpdate?.Invoke(writer);
        }


        public async Task<bool> Initialize(string auditType, CancellationToken cancellationToken)
        {
            WriterResult.OnProgressUpdate += DatalinkTest_OnProgressUpdate;
            WriterResult.OnStatusUpdate += DatalinkTest_OnStatusUpdate;
            WriterResult.RunStatus = TransformWriterResult.ERunStatus.Running;
            return await WriterResult.Initialize(cancellationToken);
        }

        /// <summary>
        /// Runs copies the datalink's current source/target tables and uses them as the basis for future test.
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> RunSnapshot(CancellationToken cancellationToken)
        {
            try
            {
                var ct = CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token,
                    cancellationToken);

                var token = ct.Token;
                token.ThrowIfCancellationRequested();
                
                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null);
                
                var percent = 0;

                foreach (var step in _datalinkTest.DexihDatalinkTestSteps.OrderBy(c => c.Position))
                {
                    percent += 100 / _datalinkTest.DexihDatalinkTestSteps.Count;

                    var datalink = _hub.DexihDatalinks.SingleOrDefault(c => c.IsValid && c.Key == step.DatalinkKey);

                    if (datalink == null)
                    {
                        throw new DatalinkTestRunException(
                            $"The datalink test {_datalinkTest.Name} failed as the datalink with the key {step.DatalinkKey} could not be found.");
                    }

                    // copy the expected results
                    if (datalink.LoadStrategy == TransformWriterTarget.ETransformWriterMethod.Bulk)
                    {
                        foreach (var target in datalink.DexihDatalinkTargets)
                        {
                            var dbDatalinkTargetTable = _hub.GetTableFromKey(target.TableKey);
                            var dbDatalinkTargetConnection =
                                _hub.DexihConnections.Single(
                                    c => c.IsValid && c.Key == dbDatalinkTargetTable.ConnectionKey);
                            
                            var targetConnection = dbDatalinkTargetConnection.GetConnection(_transformSettings);
                            var targetTable = dbDatalinkTargetTable.GetTable(_hub, targetConnection, _transformSettings);

                            var dbExpectedConnection =_hub.DexihConnections.Single(c => c.IsValid && c.Key == step.ExpectedConnectionKey);
                            var dbExpectedTable = dbDatalinkTargetTable.CloneProperties<DexihTable>();
                            dbExpectedTable.Name = step.ExpectedTableName;
                            dbExpectedTable.Schema = step.ExpectedSchema;
                            var expectedConnection = dbExpectedConnection.GetConnection(_transformSettings);
                            var expectedTable = dbExpectedTable.GetTable(_hub, targetConnection, _transformSettings);

                            UpdateProgress(percent);

                            await expectedConnection.CreateTable(expectedTable, true, token);

                            using (var targetReader = targetConnection.GetTransformReader(targetTable))
                            using (var targetReader2 = new ReaderConvertDataTypes(expectedConnection, targetReader))
                            {
                                await targetReader2.Open(0, null, token);
                                await expectedConnection.ExecuteInsertBulk(expectedTable, targetReader2, token);
                                
                                WriterResult.IncrementRowsCreated(targetReader2.TransformRows);
                                WriterResult.IncrementRowsReadPrimary(targetReader2.TotalRowsReadPrimary);
                                
                            }
                        }
                    }
                    else
                    {
                        // if there is no target table, then copy the outputs of the datalink.
                        var dbExpectedConnection =
                            _hub.DexihConnections.Single(c => c.IsValid && c.Key == step.ExpectedConnectionKey);
                        var dbExpectedTable = datalink.GetOutputTable();
                        var expectedTable = dbExpectedTable.GetTable(null, null);
                        expectedTable.Name = step.ExpectedTableName;
                        expectedTable.Schema = step.ExpectedSchema;
                        var expectedConnection = dbExpectedConnection.GetConnection(_transformSettings);

                        var transformOperations = new TransformsManager(_transformSettings);
                        var runPlan =
                            transformOperations.CreateRunPlan(_hub, datalink, null,null, null, _transformWriterOptions);

                        UpdateProgress(percent);

                        await expectedConnection.CreateTable(expectedTable, true, cancellationToken);

                        using (var transform = runPlan.sourceTransform)
                        using (var transform2 = new ReaderConvertDataTypes(expectedConnection, transform))
                        {
                            await transform2.Open(0, null, cancellationToken);
                            await expectedConnection.ExecuteInsertBulk(expectedTable, transform2, cancellationToken);
                            
                            WriterResult.IncrementRowsCreated(transform2.TransformRows);
                            WriterResult.IncrementRowsReadPrimary(transform2.TotalRowsReadPrimary);

                        }
                    }

                    foreach (var testTable in step.DexihDatalinkTestTables)
                    {
                        var dbDatalinkTable = _hub.GetTableFromKey(testTable.TableKey);
                        var dbDatalinkConnection = _hub.DexihConnections.Single(c => c.IsValid && c.Key == dbDatalinkTable.ConnectionKey);
                        var datalinkConnection = dbDatalinkConnection.GetConnection(_transformSettings);
                        var datalinkTable = dbDatalinkTable.GetTable(_hub, datalinkConnection, _transformSettings);

                        var dbSourceConnection =
                            _hub.DexihConnections.Single(c => c.IsValid && c.Key == testTable.SourceConnectionKey);
                        var dbSourceTable = dbDatalinkTable.CloneProperties<DexihTable>();
                        dbSourceTable.Name = testTable.SourceTableName;
                        dbSourceTable.Schema = testTable.SourceSchema;
                        var testConnection = dbSourceConnection.GetConnection(_transformSettings);
                        var testTable1 = dbSourceTable.GetTable(_hub, testConnection, _transformSettings);


                        UpdateProgress(percent);


                        await testConnection.CreateTable(testTable1, true, cancellationToken);

                        using (var datalinkReader = datalinkConnection.GetTransformReader(datalinkTable))
                        using (var datalinkReader2 = new ReaderConvertDataTypes(testConnection, datalinkReader))
                        {
                            await datalinkReader2.Open(0, null, cancellationToken);
                            await testConnection.ExecuteInsertBulk(testTable1, datalinkReader2, cancellationToken);
                            
                            WriterResult.IncrementRowsCreated(datalinkReader2.TransformRows);
                            WriterResult.IncrementRowsReadPrimary(datalinkReader2.TotalRowsReadPrimary);
                        }
                    }
                }

                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Finished, "Finished", null);
                await WriterResult.CompleteDatabaseWrites();
                return true;
            }
            catch (Exception ex)
            {
                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, ex.Message, ex);
                return false;
            }
        }
        
        /// <summary>
        /// Runs the datalink test and returns the results
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="DatalinkTestRunException"></exception>
        public async Task<List<TestResult>> Run(CancellationToken cancellationToken)
        {
            try
            {
                var ct = CancellationTokenSource.CreateLinkedTokenSource(_cancellationTokenSource.Token,
                    cancellationToken);

                var token = ct.Token;
                token.ThrowIfCancellationRequested();
                
                var tempTargetTableKey = -10000;
                
                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Started, null, null);

                var passed = 0;
                var failed = 0;

                foreach (var step in _datalinkTest.DexihDatalinkTestSteps.OrderBy(c => c.Position))
                {
                    var datalink = _hub.DexihDatalinks.SingleOrDefault(c => c.IsValid && c.Key == step.DatalinkKey);

                    if (datalink == null)
                    {
                        throw new DatalinkTestRunException( $"The datalink test {_datalinkTest.Name} failed as the datalink with the key {step.DatalinkKey} could not be found.");
                    }

                    UpdateProgress(1);

                    // prepare all the relevant tables
                    foreach (var testTable in step.DexihDatalinkTestTables)
                    {
                        await PrepareTestTable(testTable, token);
                    }

                    UpdateProgress(2);

                    datalink.AuditConnectionKey = _datalinkTest.AuditConnectionKey;

                    var dexihTargetConnection = _hub.DexihConnections.Single(c => c.IsValid && c.Key == step.TargetConnectionKey);
                    ICollection<DexihTable> targetTables;

                    // add a target table to store the data when the datalink doesn't have one.
                    if (!datalink.DexihDatalinkTargets.Any())
                    {
                        var target = new DexihDatalinkTarget()
                        {
                            TableKey = tempTargetTableKey--,
                        };
                        
                        datalink.DexihDatalinkTargets.Add(target);
                        datalink.UpdateStrategy = TransformDelta.EUpdateStrategy.Reload;
                        datalink.LoadStrategy = TransformWriterTarget.ETransformWriterMethod.Bulk;

                        // var targetTable = datalink.GetOutputTable();
                        var table = new DexihTable()
                        {
                            Key = target.TableKey,
                            DexihTableColumns = datalink.GetOutputTable().DexihDatalinkColumns
                                .Select(c => c.CloneProperties<DexihTableColumn>()).ToArray()
                        };

                        // dexihTargetConnection.DexihTables.Add(table);
                        targetTables = new List<DexihTable> {table};
                    }
                    else
                    {
                        targetTables = datalink.DexihDatalinkTargets.Select(c => _hub.GetTableFromKey(c.TableKey)).ToList();
                    }

                    if (targetTables.Count > 1)
                    {
                        throw new DatalinkTestRunException("Currently datalink tests can only be used with datalinks containing no more than one target table.");
                    }

                    foreach (var table in targetTables)
                    {
                        table.ConnectionKey = dexihTargetConnection.Key;
                        table.Name = step.TargetTableName;
                        table.Schema = step.TargetSchema;
                    }

                    UpdateProgress(50);

                    // run the datalink
                    var datalinkRun = new DatalinkRun(_transformSettings, _logger, WriterResult.AuditKey, datalink, _hub, null, _transformWriterOptions);
                    datalinkRun.WriterTarget.WriterResult.AuditType = "DatalinkTestStep";
                    datalinkRun.WriterTarget.WriterResult.ReferenceKey = step.Key;

                    // await datalinkRun.Initialize(cancellationToken);
                    datalinkRun.Build(token);
                    await datalinkRun.Run(token);

                    UpdateProgress(70);

                    foreach (var table in targetTables)
                    {
                        var testResult = new TestResult()
                        {
                            Name = step.Name,
                            StartDate = DateTime.Now,
                            TestStepKey = step.Key
                        };
                        
                        var dexihExpectedConnection = _hub.DexihConnections.Single(c => c.IsValid && c.Key == step.ExpectedConnectionKey);
                        var dexihExpectedTable = table.CloneProperties<DexihTable>();
                        dexihExpectedTable.ConnectionKey = dexihExpectedConnection.Key;
                        dexihExpectedTable.Name = step.ExpectedTableName;
                        dexihExpectedTable.Schema = step.ExpectedSchema;

                        var expectedConnection = dexihExpectedConnection.GetConnection(_transformSettings);
                        var expectedTable = dexihExpectedTable.GetTable(_hub, expectedConnection, _transformSettings);
                        var expectedTransform = expectedConnection.GetTransformReader(expectedTable);

                        var targetConnection = dexihTargetConnection.GetConnection(_transformSettings);
                        var targetTable = table.GetTable(_hub, targetConnection, _transformSettings);
                        var targetTransform = targetConnection.GetTransformReader(targetTable);

                        // the error table is used to store any rows which do not match.
                        var dexihErrorConnection = _hub.DexihConnections.SingleOrDefault(c => c.IsValid && c.Key == step.ErrorConnectionKey);
                        Connection errorConnection = null;
                        Table errorTable = null;
                        if (dexihErrorConnection != null)
                        {
                            var dexihErrorTable = table.CloneProperties<DexihTable>();
                            dexihErrorTable.ConnectionKey = dexihErrorConnection.Key;
                            dexihErrorTable.Name = step.ErrorTableName;
                            dexihErrorTable.Schema = step.ErrorSchema;
                            errorConnection = dexihErrorConnection.GetConnection(_transformSettings);
                            errorTable = dexihErrorTable.GetTable(_hub, errorConnection, _transformSettings);

                            foreach (var column in errorTable.Columns)
                            {
                                column.DeltaType = EDeltaType.NonTrackingField;
                            }

                            errorTable.Columns.Add(new TableColumn("error_audit_key", ETypeCode.Int64,
                                EDeltaType.CreateAuditKey));
                            errorTable.Columns.Add(new TableColumn("error_operation", ETypeCode.CharArray,
                                EDeltaType.DatabaseOperation) {MaxLength = 1});

                        }

                        // use the delta transform to compare expected and target tables.
                        var delta = new TransformDelta(targetTransform, expectedTransform,
                            TransformDelta.EUpdateStrategy.AppendUpdateDelete, 0, false);

                        await delta.Open(0, null, token);

                        testResult.RowsMismatching = 0;
                        testResult.RowsMissingFromSource = 0;
                        testResult.RowsMissingFromTarget = 0;

                        var operationColumn = delta.CacheTable.Columns.GetOrdinal(EDeltaType.DatabaseOperation);
                        
                        var errorCache = new TableCache();

                        // loop through the delta.  any rows which don't match on source/target should filter through, others will be ignored.
                        while (await delta.ReadAsync(token))
                        {
                            testResult.TestPassed = false;
                            switch (delta[operationColumn])
                            {
                                case 'C':
                                    testResult.RowsMissingFromTarget++;
                                    break;
                                case 'U':
                                    testResult.RowsMismatching++;
                                    break;
                                case 'D':
                                    testResult.RowsMissingFromSource++;
                                    break;
                            }
                            datalinkRun.WriterTarget.WriterResult.Failed++;
                            WriterResult.Failed++;
                            WriterResult.IncrementRowsCreated();

                            if (errorTable != null && errorCache.Count < MaxErrorRows)
                            {
                                var row = new object[errorTable.Columns.Count];

                                for (var i = 0; i < errorTable.Columns.Count; i++)
                                {
                                    var column = errorTable[i];
                                    switch (column.DeltaType)
                                    {
                                        case EDeltaType.CreateAuditKey:
                                            row[i] = datalinkRun.WriterTarget.WriterResult.AuditKey;
                                            break;
                                        case EDeltaType.DatabaseOperation:
                                            row[i] = delta[operationColumn];
                                            break;
                                        default:
                                            row[i] = delta[column.Name];
                                            break;
                                    }
                                }

                                errorCache.Add(row);
                            }
                        }

                        if (errorCache.Count > 0)
                        {
                            errorTable.Data = errorCache;
                            var createReader = new ReaderMemory(errorTable);
                            
                            if (!await errorConnection.TableExists(errorTable, cancellationToken))
                            {
                                await errorConnection.CreateTable(errorTable, false, cancellationToken);    
                            }
                            
                            await errorConnection.ExecuteInsertBulk(errorTable, createReader, cancellationToken);
                        }

                        WriterResult.RowsIgnored += delta.TotalRowsIgnored;
                        WriterResult.RowsPreserved += delta.TotalRowsPreserved;

                        if (testResult.TestPassed == false)
                        {
                            failed++;
                        }
                        else
                        {
                            passed++;
                        }

                        if (datalinkRun.WriterTarget.WriterResult.RunStatus == TransformWriterResult.ERunStatus.Finished)
                        {
                            if (testResult.TestPassed)
                            {
                                datalinkRun.WriterTarget.WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Passed, "Datalink test passed", null);
                            }
                            else
                            {
                                datalinkRun.WriterTarget.WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Failed, $"Datalink test failed, {testResult.RowsMissingFromSource} rows missing from expected, {testResult.RowsMissingFromTarget} rows missing from actual, {testResult.RowsMismatching} rows with mismatching columns.", null);
                            }
                        }
                        
                        TestResults.Add(testResult);
                    }
                }

                if (WriterResult.Failed > 0)
                {
                    WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Failed, $"{passed} tests passed, {failed} test failed.", null);
                }
                else
                {
                    WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Passed, $"{passed} tests passed.", null);
                }

                await WriterResult.CompleteDatabaseWrites();

                return TestResults;
                
            }
            catch (Exception ex)
            {
                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, ex.Message, ex);
                return TestResults;
            }

        }

        /// <summary>
        /// Creates test table and copies data to it.
        /// </summary>
        /// <param name="datalinkTestTable"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task PrepareTestTable(DexihDatalinkTestTable datalinkTestTable, CancellationToken cancellationToken)
        {
            
            // leave test table as is
            if (datalinkTestTable.Action == ETestTableAction.None)
            {
                return;
            }
            
            var dexihTable = _hub.GetTableFromKey(datalinkTestTable.TableKey);

            var dexihTestConnection = _hub.DexihConnections.Single(c => c.IsValid && c.Key == datalinkTestTable.SourceConnectionKey);
            var testConnection = dexihTestConnection.GetConnection(_transformSettings);
            var testTable = dexihTable.GetTable(_hub, testConnection, _transformSettings);

            // set the test table with the testing name/schema
            testTable.Name = datalinkTestTable.TestTableName;
            testTable.Schema = datalinkTestTable.TestSchema;

            var testTableExists = await testConnection.TableExists(testTable, cancellationToken);

            if (testTableExists && (datalinkTestTable.Action == ETestTableAction.Truncate ||
                                    datalinkTestTable.Action == ETestTableAction.TruncateCopy))
            {
                await testConnection.TruncateTable(testTable, cancellationToken);
            }
            else
            {
                // drop and re-create the table.
                await testConnection.CreateTable(testTable, true, cancellationToken);
            }

            if (datalinkTestTable.Action == ETestTableAction.Truncate ||
                datalinkTestTable.Action == ETestTableAction.DropCreate)
            {
                return;
            }

            var dexihSourceConnection = _hub.DexihConnections.Single(c => c.IsValid && c.Key == datalinkTestTable.SourceConnectionKey);
            var sourceConnection = dexihSourceConnection.GetConnection(_transformSettings);
            var sourceTable = dexihTable.GetTable(_hub, sourceConnection, _transformSettings);

            // set the source table with the testing name
            sourceTable.Name = datalinkTestTable.SourceTableName;
            sourceTable.Schema = datalinkTestTable.SourceSchema;

            var sourceTableExists = await sourceConnection.TableExists(sourceTable, cancellationToken);
            if (!sourceTableExists)
            {
                throw new DatalinkTestRunException($"The datalink test {_datalinkTest.Name} failed as the table {sourceTable.Schema}.{sourceTable.Name} does not exist in the source connection.");
            }

            // copy the test data across.

            using (var writer = new TransformWriterTarget(testConnection, testTable))
            {
                await writer.WriteRecordsAsync(sourceConnection.GetTransformReader(sourceTable), TransformDelta.EUpdateStrategy.Reload, cancellationToken);    
            }
            

            // update the dexihtable with the test connection, so the datalink runs against this value.
            dexihTable.ConnectionKey = datalinkTestTable.TestConnectionKey;
            dexihTable.Name = datalinkTestTable.TestTableName;
            dexihTable.Schema = datalinkTestTable.TestSchema;

        }
        
        public object Data { get => WriterResult; set => throw new NotSupportedException(); }


        public void Dispose()
        {
        }

        public async Task StartAsync(ManagedTaskProgress progress, CancellationToken cancellationToken = default)
        {
            void ProgressUpdate(TransformWriterResult writerResult)
            {
                progress.Report(writerResult.PercentageComplete, writerResult.Passed + writerResult.Failed, writerResult.IsFinished ? "" : "Running datalink tests...");
            }

            OnProgressUpdate += ProgressUpdate;
                            
            await Initialize("DatalinkTest", cancellationToken);

            progress.Report(0, 0, $"Running datalink test {_datalinkTest.Name}...");

            switch (StartMode)
            {
                case EStartMode.RunSnapshot:
                    await RunSnapshot(cancellationToken);
                    break;
                case EStartMode.RunTests:
                    await Run(cancellationToken);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        public void Cancel()
        {
            _cancellationTokenSource.Cancel();
        }

        public void Schedule(DateTime startsAt, CancellationToken cancellationToken = default)
        {
            
        }
    }
}