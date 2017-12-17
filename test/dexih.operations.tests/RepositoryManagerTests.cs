using dexih.repository;
using Microsoft.EntityFrameworkCore;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Microsoft.Extensions.Logging;
using static Dexih.Utils.DataType.DataType;
using System;
using Dexih.Utils.CopyProperties;
using static dexih.repository.DexihDatalinkTable;

namespace dexih.operations.tests
{
    public class DatabaseTests
    {
		private readonly ILoggerFactory _loggerFactory;

        public DatabaseTests()
        {
			_loggerFactory = new LoggerFactory();
        }

        public async Task<DexihRepositoryContext> CreateRepositoryEmpty()
        {
            var factory = new TestContextFactory();
            //var context = factory.CreateProgreSql<DexihRepositoryContext>(true);
            var context = factory.CreateSqlServer<DexihRepositoryContext>(true);
            var seed = new SeedData();

            await seed.UpdateReferenceData(context, null, null);

            return context;
        }

        /// <summary>
        /// Create a basic repository with hub.
        /// </summary>
        /// <returns></returns>
        public async Task<RepositoryManager> CreateRepositoryWithHub()
        {
            var context = await CreateRepositoryEmpty();
            var database = new RepositoryManager("key", context, _loggerFactory);
            var hub = new DexihHub()
            {
                Name = "test",
                Description = "hub description",
                EncryptionKey = "123"
            };
            var newHub = await database.SaveHub(hub);

            Assert.Equal(1, context.DexihHubs.Where(c=>!c.IsInternal).Count());
            Assert.True(await database.GetInternalConnectionKey(newHub.HubKey) > 0);

            return database;
        }

        public async Task<RepositoryManager> CreateRepositoryWithConnections()
        {
            var database = await CreateRepositoryWithHub();
            var hub = await database.DbContext.DexihHubs.SingleAsync(c => c.Name == "test" && c.IsValid);
            var connection = new DexihConnection()
            {
                Name = "source",
                Purpose = DexihConnection.EConnectionPurpose.Source,
                Server = "source server",
                DatabaseTypeKey = database.DbContext.DexihDatabaseTypes.First().DatabaseTypeKey,
                HubKey = hub.HubKey
            };

            var saveConnectionResult = await database.SaveConnection(hub.HubKey, connection);

            connection = new DexihConnection()
            {
                Name = "managed",
                Purpose = DexihConnection.EConnectionPurpose.Managed,
                Server = "managed server",
                DatabaseTypeKey = database.DbContext.DexihDatabaseTypes.First().DatabaseTypeKey,
                HubKey = hub.HubKey
            };

            saveConnectionResult = await database.SaveConnection(hub.HubKey, connection);

            var dbSourceConnection = database.DbContext.DexihConnections.Single(c => c.Purpose == DexihConnection.EConnectionPurpose.Source && c.IsValid);
            Assert.Equal("source", dbSourceConnection.Name);
            Assert.Equal("source server", dbSourceConnection.Server);

            var dbManagedConnection = database.DbContext.DexihConnections.Single(c => c.Purpose == DexihConnection.EConnectionPurpose.Managed && c.IsValid);
            Assert.Equal("managed", dbManagedConnection.Name);
            Assert.Equal("managed server", dbManagedConnection.Server);

            return database;
        }

        public async Task<RepositoryManager> CreateRepositoryWithSourceTable()
        {
            var database = await CreateRepositoryWithConnections();
            var dbConnection = await database.DbContext.DexihConnections.FirstAsync(c => c.Name == "source" && c.IsValid);

            var table = new DexihTable()
            {
	            Name = "table1",
                Description = "table description",
                BaseTableName = "table1",
                DexihTableColumns = new DexihTableColumn[] {
                    new DexihTableColumn() { Name = "key1", LogicalName = "key1", Datatype = ETypeCode.String, DeltaType = functions.TableColumn.EDeltaType.NaturalKey },
                    new DexihTableColumn() { Name = "natural key", LogicalName = "natural key", Datatype = ETypeCode.Int32, DeltaType = functions.TableColumn.EDeltaType.TrackingField },
                    new DexihTableColumn() { Name = "ignore", LogicalName = "ignore", Datatype = ETypeCode.Int32, DeltaType = functions.TableColumn.EDeltaType.IgnoreField }
                },
                ConnectionKey = dbConnection.ConnectionKey,
				HubKey = dbConnection.HubKey

            };

			await database.SaveTables(dbConnection.HubKey, new DexihTable[] { table }, true, false);

            return database;
        }



        [Fact]
        public async Task CreateRepository()
        {
            using (var context = await CreateRepositoryEmpty())
            {
                Assert.True(context.DexihDatabaseTypes.Count() > 0);
                Assert.True(context.DexihProfileRules.Count() > 0);
                Assert.True(context.DexihStandardFunctions.Count() > 0);
                Assert.True(context.DexihTransforms.Count() > 0);
            }
        }

        [Fact]
        public async Task NewHub()
        {
            using (var database = await CreateRepositoryWithHub())
            {
            }
        }

        [Fact]
        public async Task NewConnection()
        {
            //test 1, create initial connection
            using (var database = await CreateRepositoryWithConnections())
            {
                var dbConnection = await database.DbContext.DexihConnections.SingleOrDefaultAsync(c => c.Name == "source" && c.IsValid);
                Assert.Equal("source", dbConnection.Name);
                Assert.Equal("source server", dbConnection.Server);

                //test2: modify the connection and resave
                dbConnection.Server = "source server 2";
                var saveConnectionResult = await database.SaveConnection(dbConnection.HubKey, dbConnection);
                Assert.NotNull(saveConnectionResult);

                dbConnection = database.DbContext.DexihConnections.Single(c => c.ConnectionKey == saveConnectionResult.ConnectionKey && c.IsValid);
                Assert.Equal("source", dbConnection.Name);
                Assert.Equal("source server 2", dbConnection.Server);

                //test3: create a new connection with the same name.  save should fail.
                //test 1: create a new connection and save it
                var connection = new DexihConnection()
                {
                    Name = "source",
                    Server = "source server 3",
                    DatabaseTypeKey = database.DbContext.DexihDatabaseTypes.First().DatabaseTypeKey
                };

                Assert.Throws<AggregateException>(() => database.SaveConnection(dbConnection.HubKey, connection).Result);

				// test4: delete the connection.
				var deleteReturn = await database.DeleteConnections(dbConnection.HubKey, new long[] { dbConnection.ConnectionKey });
				Assert.NotNull(deleteReturn);

				// check the connection is deleted
				var dbConnection2 = await database.DbContext.DexihConnections.SingleOrDefaultAsync(c => c.ConnectionKey == dbConnection.ConnectionKey && c.IsValid);
				Assert.Null(dbConnection2);

				//test 5: create a new connection with the same name as the deleted one.  save should succeed
				saveConnectionResult = await database.SaveConnection(dbConnection.HubKey, connection);
				Assert.NotNull(saveConnectionResult);

			}
        }

        [Fact]
        public async Task NewTable()
        {
            //test 1, create initial table
            using (var database = await CreateRepositoryWithSourceTable())
            {
                var dbConnection = await database.DbContext.DexihConnections.FirstAsync(c => c.Name == "source" && c.IsValid);

                var dbTable = await database.DbContext.DexihTables.Include(d => d.DexihTableColumns).SingleOrDefaultAsync(c => c.ConnectionKey == dbConnection.ConnectionKey);

                Assert.NotNull(dbTable);
                Assert.Equal("table1", dbTable.Name);
                Assert.Equal(3, dbTable.DexihTableColumns.Count());


                //test2: update the table, and check update has succeeded.
                dbTable.Name = "updated table";
				var saveResult = await database.SaveTables(dbConnection.HubKey, new DexihTable[] { dbTable }, true, false);
                Assert.NotNull(saveResult);

                var dbTable2 = await database.DbContext.DexihTables.Include(d => d.DexihTableColumns).SingleOrDefaultAsync(c => c.TableKey == dbTable.TableKey && c.IsValid);
                Assert.Equal("updated table", dbTable2.Name);

                //test3: create a new table with the same name.  save should fail
                var duplicateTable = new DexihTable()
                {
	                Name = "updated table",
                    ConnectionKey = dbConnection.ConnectionKey,
                    BaseTableName = "updated table",
					HubKey = dbConnection.HubKey
                };

                Assert.Throws<AggregateException>(() => database.SaveTables(dbConnection.HubKey, new DexihTable[] { duplicateTable }, true, false).Result);

				// test4: delete the table.
				var deleteReturn = await database.DeleteTables(dbConnection.HubKey, new long[] { dbTable.TableKey });
				Assert.NotNull(deleteReturn);

				// check the table is deleted
				dbTable = await database.DbContext.DexihTables.SingleOrDefaultAsync(c => c.TableKey == dbTable2.TableKey && c.IsValid);
				Assert.Null(dbTable);

				//test 5: create a new table with the same name as the deleted one.  save should succeed
				var saveResult2 = await database.SaveTables(dbConnection.HubKey, new DexihTable[] {duplicateTable }, true, false);
				Assert.NotNull(saveResult2);
			}
        }

		[Fact]
		public async Task NewFileFormat()
		{
			//test 1, create initial table
			using (var database = await CreateRepositoryWithSourceTable())
			{
				var dbConnection = await database.DbContext.DexihConnections.SingleOrDefaultAsync(c => c.Name == "source" && c.IsValid);

				var fileFormat = new DexihFileFormat()
				{
					Name = "new format",
					HubKey = dbConnection.HubKey,
					BufferSize = 100,
					Comment = '-',
					Delimiter = "|",
					Description = "the new format",
					HasHeaderRecord = true,
					IsDefault = true,
				};

				var saveFileResult = await database.SaveFileFormat(dbConnection.HubKey, fileFormat);

				Assert.NotNull(saveFileResult);

				// lookup the fileformat and check save was correct.
				var fileFormat2 = saveFileResult;
				var fileFormat3 = await database.DbContext.DexihFileFormat.SingleAsync(c => c.FileFormatKey == fileFormat2.FileFormatKey && c.IsValid);

				Assert.Equal("new format", fileFormat3.Name);
				Assert.Equal(100, fileFormat3.BufferSize);
				Assert.Equal('-', fileFormat3.Comment);
				Assert.Equal("|", fileFormat3.Delimiter);
				Assert.Equal("the new format", fileFormat3.Description);
				Assert.True(fileFormat3.HasHeaderRecord);
				Assert.True(fileFormat3.IsDefault);

				//test2: update the fileformat, and check update has succeeded.
				fileFormat3.Name = "updated format";
				var saveResult = await database.SaveFileFormat(dbConnection.HubKey, fileFormat3);
				Assert.NotNull(saveResult);

				var fileFormat4 = await database.DbContext.DexihFileFormat.SingleAsync(c => c.FileFormatKey == fileFormat2.FileFormatKey && c.IsValid);
				Assert.Equal("updated format", fileFormat4.Name);

				//test3: create a new table with the same name.  save should fail
				var duplicateFormat = new DexihFileFormat()
				{
					Name = "updated format",
					HubKey = dbConnection.HubKey,
					BufferSize = 100,
					Comment = '-',
					Delimiter = ",",
				};

                Assert.Throws<AggregateException>(() => database.SaveFileFormat(dbConnection.HubKey, duplicateFormat).Result);

				// test4: delete the fileformat.
				var deleteReturn = await database.DeleteFileFormats(dbConnection.HubKey, new long[] { fileFormat3.FileFormatKey });
				Assert.NotNull(deleteReturn);

				// check the fileformat is deleted
				var fileFormat5 = await database.DbContext.DexihFileFormat.SingleOrDefaultAsync(c => c.FileFormatKey ==fileFormat3.FileFormatKey && c.IsValid);
				Assert.Null(fileFormat5);

				//test 5: create a new fileformat with the same name as the deleted one.  save should succeed
				var saveResult2 = await database.SaveFileFormat(dbConnection.HubKey, duplicateFormat);
				Assert.NotNull(saveResult2);

			}
		}

		[Fact]
		public async Task NewValidation()
		{
			//test 1, create initial table
			using (var database = await CreateRepositoryWithSourceTable())
			{
				var dbConnection = await database.DbContext.DexihConnections.SingleOrDefaultAsync(c => c.Name == "source" && c.IsValid);
				var lookupColumn = await database.DbContext.DexihTableColumns.FirstAsync();

				var validation = new DexihColumnValidation()
				{
					Name = "new validation",
					HubKey = dbConnection.HubKey,
					AllowDbNull = true,
					CleanAction = DexihColumnValidation.ECleanAction.DefaultValue,
					Datatype = ETypeCode.String,
					DefaultValue = "default",
					Description = "validation description",
					InvalidAction = functions.Function.EInvalidAction.Clean,
					ListOfValues = new [] {"abc", "ndef", "nhij"},
					ListOfNotValues = new [] {"jkl", "nnmo", "npqr"},
					LookupIsValid = true,
					LookupMultipleRecords = true,
					MaxLength = 10,
					MaxValue = 20,
					MinLength = 30,
					MinValue = 40,
					PatternMatch = "pattern",
					RegexMatch = "regex",
					LookupColumnKey = lookupColumn.ColumnKey
				};

				var saveResult = await database.SaveColumnValidation(dbConnection.HubKey, validation);

				Assert.NotNull(saveResult);

				// lookup the validation and check savde was correctly.
				var validation2 = saveResult;
				var validation3 = await database.DbContext.DexihColumnValidation.SingleAsync(c => c.ColumnValidationKey == validation2.ColumnValidationKey && c.IsValid);

				Assert.Equal("new validation", validation3.Name);
				Assert.Equal(dbConnection.HubKey, validation3.HubKey);
				Assert.True(validation3.AllowDbNull);
				Assert.Equal(DexihColumnValidation.ECleanAction.DefaultValue, validation3.CleanAction);
				Assert.Equal(ETypeCode.String, validation3.Datatype);
				Assert.Equal("default", validation3.DefaultValue);
				Assert.Equal("validation description", validation3.Description);
				Assert.Equal(functions.Function.EInvalidAction.Clean, validation3.InvalidAction);
				Assert.Equal(new [] {"abc", "ndef", "nhij"}, validation3.ListOfValues);
				Assert.Equal(new [] {"jkl", "nnmo", "npqr"}, validation3.ListOfNotValues);
				Assert.True(validation3.LookupIsValid);
				Assert.True(validation3.LookupMultipleRecords);
				Assert.Equal(10, validation3.MaxLength);
				Assert.Equal(20, validation3.MaxValue);
				Assert.Equal(30, validation3.MinLength);
				Assert.Equal(40, validation3.MinValue);
				Assert.Equal("pattern", validation3.PatternMatch);
				Assert.Equal("regex", validation3.RegexMatch);
				Assert.Equal(lookupColumn.ColumnKey, validation3.LookupColumnKey);

				//test2: update the fileformat, and check update has succeeded.
				validation3.Name = "updated validation";
				var saveResult2 = await database.SaveColumnValidation(dbConnection.HubKey, validation3);
				Assert.NotNull(saveResult2);

				var validation4 = await database.DbContext.DexihColumnValidation.SingleAsync(c => c.ColumnValidationKey == validation2.ColumnValidationKey && c.IsValid);
				Assert.Equal("updated validation", validation4.Name);

				//test3: create a new table with the same name.  save should fail
				var duplicate = new DexihColumnValidation()
				{
					Name = "updated validation",
					HubKey = dbConnection.HubKey,
					Datatype = ETypeCode.String,
				};

                Assert.Throws<AggregateException>(() => database.SaveColumnValidation(dbConnection.HubKey, duplicate).Result);

				// test4: delete the fileformat.
				var deleteReturn = await database.DeleteColumnValidations(dbConnection.HubKey, new long[] { validation3.ColumnValidationKey });
				Assert.NotNull(deleteReturn);

				// check the fileformat is deleted
				var validation5 = await database.DbContext.DexihColumnValidation.SingleOrDefaultAsync(c => c.ColumnValidationKey == validation3.ColumnValidationKey && c.IsValid);
				Assert.Null(validation5);

				//test 5: create a new valiation with the same name as the deleted one.  save should succeed
				var saveResult3 = await database.SaveColumnValidation(dbConnection.HubKey, duplicate);
				Assert.NotNull(saveResult3);
			}
		}

        [Fact]
        public async Task NewDatalink()
        {
            //test 1, create initial table
            using (var database = await CreateRepositoryWithSourceTable())
            {
                var dbSourceConnection = await database.DbContext.DexihConnections.SingleOrDefaultAsync(c => c.Name == "source" && c.IsValid);
                var dbManagedConnection = await database.DbContext.DexihConnections.SingleOrDefaultAsync(c => c.Name == "managed" && c.IsValid);
                var dbSourceTable = await database.DbContext.DexihTables.FirstAsync(c => c.Name == "table1" && c.IsValid);

                var newDatalinks = await database.NewDatalinks(
                    dbSourceConnection.HubKey, 
                    null, 
                    DexihDatalink.EDatalinkType.Stage, 
                    dbManagedConnection.ConnectionKey, 
                    new long[] { dbSourceTable.TableKey }, 
                    null, 
                    null, 
                    dbManagedConnection.ConnectionKey
                    );

                Assert.Single(newDatalinks);

                var newDatalink = newDatalinks[0];

                Assert.NotNull(newDatalink);

                var dbDatalink = await database.GetDatalink(newDatalink.HubKey, newDatalink.DatalinkKey);

                // check the datalink properties are set.
                Assert.False(dbDatalink.AddDefaultRow);
                Assert.Equal(dbManagedConnection.ConnectionKey, dbDatalink.AuditConnectionKey);
                Assert.Equal(DexihDatalink.EDatalinkType.Stage, dbDatalink.DatalinkType);
                Assert.Equal(1, dbDatalink.DexihDatalinkTransforms.Count);
                Assert.Equal(dbSourceConnection.HubKey, dbDatalink.HubKey);
                Assert.False(dbDatalink.IsShared);
                Assert.True(dbDatalink.IsValid);
                Assert.False(dbDatalink.NoDataload);
                Assert.False(dbDatalink.RollbackOnFail);
                Assert.Null(dbDatalink.SourceDatalinkTable.SourceDatalinkKey);
                Assert.Equal(dbSourceTable.TableKey, dbDatalink.SourceDatalinkTable.SourceTableKey);
                Assert.Equal(ESourceType.Table, dbDatalink.SourceDatalinkTable.SourceType);
                Assert.True(dbDatalink.TargetTableKey > 0);
                Assert.Equal(transforms.TransformDelta.EUpdateStrategy.AppendUpdateDelete, dbDatalink.UpdateStrategy.Strategy);
                Assert.False(dbDatalink.VirtualTargetTable);

                // check target table created properly.
                var dbTable = await database.GetTable(newDatalink.HubKey, (long)newDatalink.TargetTableKey, true);

                Assert.Equal("stgtable1", dbTable.Name);
                Assert.Equal(dbSourceTable.Name, dbTable.BaseTableName);

                //should be no sourcesurrgate key, as source table has no surrogate key
                Assert.Null(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == functions.TableColumn.EDeltaType.SourceSurrogateKey));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == functions.TableColumn.EDeltaType.SurrogateKey));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == functions.TableColumn.EDeltaType.CreateAuditKey));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == functions.TableColumn.EDeltaType.UpdateAuditKey));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == functions.TableColumn.EDeltaType.CreateDate));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == functions.TableColumn.EDeltaType.UpdateDate));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == functions.TableColumn.EDeltaType.IsCurrentField));

                // check target columns were created
                foreach(var col in dbSourceTable.DexihTableColumns)
                {
                    if(col.DeltaType == functions.TableColumn.EDeltaType.IgnoreField)
                    {
                        Assert.Null(dbTable.DexihTableColumns.SingleOrDefault(c => c.Name == dbManagedConnection.DatabaseType.RemoveUnsupportedCharacaters(col.Name)));
                    }
                    else
                    {
                        var name = dbManagedConnection.DatabaseType.RemoveUnsupportedCharacaters(col.Name);
                        var targetColumn = dbTable.DexihTableColumns.SingleOrDefault(c => c.Name == name);
                        Assert.NotNull(targetColumn);

                        //if the target column name is cleaned and not the same, then a mapping should be created.
                        if(name != col.Name)
                        {
                            var datalinkTransform = dbDatalink.DexihDatalinkTransforms.SingleOrDefault(c => c.Transform.TransformType == DexihTransform.ETransformType.Mapping);
                            Assert.NotNull(datalinkTransform);

                            var mapping = datalinkTransform.DexihDatalinkTransformItems.SingleOrDefault(c => c.SourceDatalinkColumn.Name == col.Name);
                            Assert.NotNull(mapping);
                            Assert.Equal(targetColumn.Name, mapping.TargetDatalinkColumn.Name);
                        }
                    }
                }

                dbDatalink.Name = "new name";
                await database.SaveDatalinks(dbSourceConnection.HubKey, new DexihDatalink[] { dbDatalink }, false);
                var dbDatalink2 = await database.GetDatalink(dbDatalink.HubKey, dbDatalink.DatalinkKey);
                Assert.Equal("new name", dbDatalink2.Name);

                var dbDatalink3 = new DexihDatalink();
                dbDatalink2.CopyProperties(dbDatalink3);
                dbDatalink3.Name = "newer name";
                await database.SaveDatalinks(dbSourceConnection.HubKey, new DexihDatalink[] { dbDatalink3 }, false);
                var dbDatalink4 = await database.GetDatalink(dbDatalink.HubKey, dbDatalink.DatalinkKey);
                Assert.Equal("newer name", dbDatalink4.Name);

            }
        }
    }
}
