using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.repository;
using dexih.transforms;
using Dexih.Utils.CopyProperties;
using Microsoft.Extensions.Logging;

namespace dexih.operations
{
    public class DatalinkTestRun
    {        
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

        public List<TestResult> TestResults;

        private readonly ILogger _logger;

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
                    _hub.DexihConnections.SingleOrDefault(c => c.ConnectionKey == datalinkTest.AuditConnectionKey);

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
                ReferenceKey = datalinkTest.DatalinkTestKey,
                ParentAuditKey = 0,
                ReferenceName = datalinkTest.Name,
                SourceTableKey = 0,
                SourceTableName = "",
                TransformWriterOptions = _transformWriterOptions
            };
           
        }

        private void UpdateProgress(int percent, string message, CancellationToken cancellationToken)
        {
            WriterResult.RowsCreated = percent;
            WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Running, message, null, cancellationToken);
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
                var percent = 0;

                foreach (var step in _datalinkTest.DexihDatalinkTestSteps.OrderBy(c => c.Position))
                {
                    WriterResult.RowsTotal = 1;
                    percent += 100 / _datalinkTest.DexihDatalinkTestSteps.Count;

                    var datalink = _hub.DexihDatalinks.SingleOrDefault(c => c.DatalinkKey == step.DatalinkKey);

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
                                    c => c.ConnectionKey == dbDatalinkTargetTable.ConnectionKey);
                            var targetConnection = dbDatalinkTargetConnection.GetConnection(_transformSettings);
                            var targetTable = dbDatalinkTargetTable.GetTable(targetConnection, _transformSettings);
                            using (var targetReader = targetConnection.GetTransformReader(targetTable))
                            {
                                await targetReader.Open(0, null, cancellationToken);

                                var dbExpectedConnection =
                                    _hub.DexihConnections.Single(c => c.ConnectionKey == step.ExpectedConnectionKey);
                                var dbExpectedTable = dbDatalinkTargetTable.CloneProperties<DexihTable>();
                                dbExpectedTable.Name = step.ExpectedTableName;
                                dbExpectedTable.Schema = step.ExpectedSchema;
                                var expectedConnection = dbExpectedConnection.GetConnection(_transformSettings);
                                var expectedTable = dbExpectedTable.GetTable(targetConnection, _transformSettings);

                                UpdateProgress(percent,
                                    $"Copying table {targetTable.Name} to the expected test data table {expectedTable.Name}.",
                                    cancellationToken);

                                await expectedConnection.CreateTable(expectedTable, true, cancellationToken);
                                await expectedConnection.ExecuteInsertBulk(expectedTable, targetReader,
                                    cancellationToken);
                            }
                        }
                    }
                    else
                    {
                        // if there is no target table, then copy the outputs of the datalink.
                        var dbExpectedConnection =
                            _hub.DexihConnections.Single(c => c.ConnectionKey == step.ExpectedConnectionKey);
                        var dbExpectedTable = datalink.GetOutputTable();
                        var expectedTable = dbExpectedTable.GetTable(null, null);
                        expectedTable.Name = step.ExpectedTableName;
                        expectedTable.Schema = step.ExpectedSchema;
                        var expectedConnection = dbExpectedConnection.GetConnection(_transformSettings);

                        var transformOperations = new TransformsManager(_transformSettings);
                        var runPlan =
                            transformOperations.CreateRunPlan(_hub, datalink, null,null, null, _transformWriterOptions);

                        using (var transform = runPlan.sourceTransform)
                        {
                            await transform.Open(0, null, cancellationToken);
                            UpdateProgress(percent,
                                $"Copying datalink {datalink.Name} output to the expected test data table {expectedTable.Name}.",
                                cancellationToken);
                            await expectedConnection.CreateTable(expectedTable, true, cancellationToken);
                            await expectedConnection.ExecuteInsertBulk(expectedTable, transform, cancellationToken);
                        }
                    }

                    foreach (var testTable in step.DexihDatalinkTestTables)
                    {
                        var dbDatalinkTable = _hub.GetTableFromKey(testTable.TableKey);
                        var dbDatalinkConnection = _hub.DexihConnections.Single(c => c.ConnectionKey == dbDatalinkTable.ConnectionKey);
                        var datalinkConnection = dbDatalinkConnection.GetConnection(_transformSettings);
                        var datalinkTable = dbDatalinkTable.GetTable(datalinkConnection, _transformSettings);

                        using (var datalinkReader = datalinkConnection.GetTransformReader(datalinkTable))
                        {
                            await datalinkReader.Open(0, null, cancellationToken);
                            var dbSourceConnection =
                                _hub.DexihConnections.Single(c => c.ConnectionKey == testTable.SourceConnectionKey);
                            var dbSourceTable = dbDatalinkTable.CloneProperties<DexihTable>();
                            dbSourceTable.Name = testTable.SourceTableName;
                            dbSourceTable.Schema = testTable.SourceSchema;
                            var testConnection = dbSourceConnection.GetConnection(_transformSettings);
                            var testTable1 = dbSourceTable.GetTable(testConnection, _transformSettings);


                            UpdateProgress(percent,
                                $"Copying table {datalinkTable.Name} to the expected source data table {testTable1.Name}.",
                                cancellationToken);
                            await testConnection.CreateTable(testTable1, true, cancellationToken);
                            await testConnection.ExecuteInsertBulk(testTable1, datalinkReader, cancellationToken);
                        }
                    }
                }

                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Finished, "Finished", null, cancellationToken);
                await WriterResult.CompleteDatabaseWrites();
                return true;
            }
            catch (Exception ex)
            {
                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, ex.Message, ex, cancellationToken);
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
                var tempTargetTableKey = -10000;

                foreach (var step in _datalinkTest.DexihDatalinkTestSteps.OrderBy(c => c.Position))
                {
                    var datalink = _hub.DexihDatalinks.SingleOrDefault(c => c.DatalinkKey == step.DatalinkKey);

                    if (datalink == null)
                    {
                        throw new DatalinkTestRunException( $"The datalink test {_datalinkTest.Name} failed as the datalink with the key {step.DatalinkKey} could not be found.");
                    }

                    UpdateProgress(1,$"Preparing test tables for datalink test {_datalinkTest.Name}, step {step.Name}", cancellationToken);

                    // prepare all the relevant tables
                    foreach (var testTable in step.DexihDatalinkTestTables)
                    {
                        await PrepareTestTable(testTable, cancellationToken);
                    }

                    UpdateProgress(1, "Preparing test tables for datalink test {_datalinkTest.Name}, step {step.Name}", cancellationToken);

                    datalink.AuditConnectionKey = _datalinkTest.AuditConnectionKey;

                    var dexihTargetConnection = _hub.DexihConnections.Single(c => c.ConnectionKey == step.TargetConnectionKey);
                    ICollection<DexihTable> targetTables;

                    // add a target table to store the data when the datalink doesn't have one.
                    if (datalink.DexihDatalinkTargets.Any())
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
                            TableKey = target.TableKey,
                            DexihTableColumns = datalink.GetOutputTable().DexihDatalinkColumns
                                .Select(c => c.CloneProperties<DexihTableColumn>()).ToArray()
                        };

                        dexihTargetConnection.DexihTables.Add(table);
                        targetTables = new List<DexihTable>() {table};
                    }
                    else
                    {
                        targetTables = datalink.DexihDatalinkTargets.Select(c => _hub.GetTableFromKey(c.TableKey)).ToList();
                    }

                    foreach (var table in targetTables)
                    {
                        table.ConnectionKey = dexihTargetConnection.ConnectionKey;
                        table.Name = step.TargetTableName;
                        table.Schema = step.TargetSchema;
                    }


                    UpdateProgress(50,
                        $"Running the datalink {datalink.Name} for datalink test {_datalinkTest.Name}, step {step.Name}",
                        cancellationToken);

                    // run the datalink
                    var datalinkRun = new DatalinkRun(_transformSettings, _logger, WriterResult.AuditKey, datalink, _hub, null, _transformWriterOptions);

                    // await datalinkRun.Initialize(cancellationToken);
                    datalinkRun.Build(cancellationToken);
                    await datalinkRun.Run(cancellationToken);

                    UpdateProgress(70, "Comparing the results for datalink test {_datalinkTest.Name}, step {step.Name}", cancellationToken);

                    foreach (var table in targetTables)
                    {
                        var testResult = new TestResult()
                        {
                            Name = step.Name,
                            StartDate = DateTime.Now,
                            TestStepKey = step.DatalinkTestStepKey
                        };
                        
                        var dexihExpectedConnection = _hub.DexihConnections.Single(c => c.ConnectionKey == step.ExpectedConnectionKey);
                        var dexihExpectedTable = table.CloneProperties<DexihTable>();
                        dexihExpectedTable.ConnectionKey = dexihExpectedConnection.ConnectionKey;
                        dexihExpectedTable.Name = step.ExpectedTableName;
                        dexihExpectedTable.Schema = step.ExpectedSchema;

                        var expectedConnection = dexihExpectedConnection.GetConnection(_transformSettings);
                        var expectedTable = dexihExpectedTable.GetTable(expectedConnection, _transformSettings);
                        var expectedTransform = expectedConnection.GetTransformReader(expectedTable);

                        var targetConnection = dexihTargetConnection.GetConnection(_transformSettings);
                        var targetTable = table.GetTable(targetConnection, _transformSettings);
                        var targetTransform = targetConnection.GetTransformReader(targetTable);

                        // use the delta transform to compare expected and target tables.
                        var delta = new TransformDelta(targetTransform, expectedTransform,
                            TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve, 0, false);

                        await delta.Open(0, null, cancellationToken);

                        testResult.RowCountMatch = true;
                        while (await delta.ReadAsync(cancellationToken))
                        {
                            testResult.RowCountMatch = false;
                            WriterResult.IncrementRowsCreated();
                        }

                        WriterResult.RowsIgnored += delta.TotalRowsIgnored;
                        WriterResult.RowsPreserved += delta.TotalRowsPreserved;

                        if (testResult.RowCountMatch)
                        {
                            WriterResult.Passed++;
                        }
                        else
                        {
                            WriterResult.Failed++;
                        }

                        TestResults.Add(testResult);
                    }
                }

                if (WriterResult.Failed > 0)
                {
                    WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Failed, $"{WriterResult.Passed} tests passed, {WriterResult.Failed} test failed.", null, cancellationToken);
                }
                else
                {
                    WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Passed, $"{WriterResult.Passed} tests passed.", null, cancellationToken);
                }

                await WriterResult.CompleteDatabaseWrites();

                return TestResults;
                
            }
            catch (Exception ex)
            {
                WriterResult.SetRunStatus(TransformWriterResult.ERunStatus.Abended, ex.Message, ex,
                    cancellationToken);
                return TestResults;
            }

        }

        /// <summary>
        /// Creates test table and copies data to it.
        /// </summary>
        /// <param name="datalinkTestTableble"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task PrepareTestTable(DexihDatalinkTestTable datalinkTestTable, CancellationToken cancellationToken)
        {
            
            // leave test table as is
            if (datalinkTestTable.Action == DexihDatalinkTestTable.ETestTableAction.None)
            {
                return;
            }
            
            var dexihTable = _hub.GetTableFromKey(datalinkTestTable.TableKey);

            var dexihTestConnection = _hub.DexihConnections.Single(c => c.ConnectionKey == datalinkTestTable.SourceConnectionKey);
            var testConnection = dexihTestConnection.GetConnection(_transformSettings);
            var testTable = dexihTable.GetTable(testConnection, _transformSettings);

            // set the test table with the testing name/schema
            testTable.Name = datalinkTestTable.TestTableName;
            testTable.Schema = datalinkTestTable.TestSchema;

            var testTableExists = await testConnection.TableExists(testTable, cancellationToken);

            if (testTableExists && (datalinkTestTable.Action == DexihDatalinkTestTable.ETestTableAction.Truncate ||
                                    datalinkTestTable.Action == DexihDatalinkTestTable.ETestTableAction.TruncateCopy))
            {
                await testConnection.TruncateTable(testTable, cancellationToken);
            }
            else
            {
                // drop and re-create the table.
                await testConnection.CreateTable(testTable, true, cancellationToken);
            }

            if (datalinkTestTable.Action == DexihDatalinkTestTable.ETestTableAction.Truncate ||
                datalinkTestTable.Action == DexihDatalinkTestTable.ETestTableAction.DropCreate)
            {
                return;
            }

            var dexihSourceConnection = _hub.DexihConnections.Single(c => c.ConnectionKey == datalinkTestTable.SourceConnectionKey);
            var sourceConnection = dexihSourceConnection.GetConnection(_transformSettings);
            var sourceTable = dexihTable.GetTable(sourceConnection, _transformSettings);

            // set the source table with the testing name
            sourceTable.Name = datalinkTestTable.SourceTableName;
            sourceTable.Schema = datalinkTestTable.SourceSchema;

            var sourceTableExists = await sourceConnection.TableExists(sourceTable, cancellationToken);
            if (!sourceTableExists)
            {
                throw new DatalinkTestRunException($"The datalink test {_datalinkTest.Name} failed as the table {sourceTable.Schema}.{sourceTable.Name} does not exist in the source connection.");
            }

            // copy the test data across.
            var writer = new TransformWriterTarget(testConnection, testTable);
            await writer.WriteRecordsAsync(sourceConnection.GetTransformReader(sourceTable), TransformDelta.EUpdateStrategy.Reload, cancellationToken);

            // update the dexihtable with the test connection, so the datalink runs against this value.
            dexihTable.ConnectionKey = datalinkTestTable.TestConnectionKey;
            dexihTable.Name = datalinkTestTable.TestTableName;
            dexihTable.Schema = datalinkTestTable.TestSchema;

        }
    }
}