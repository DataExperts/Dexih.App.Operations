using dexih.repository;
using Microsoft.EntityFrameworkCore;
using System.Linq;
using System.Threading.Tasks;
using Xunit;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using dexih.functions;
using dexih.transforms;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using System.Net.Http;

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
            var context = factory.CreateInMemorySqlite<DexihRepositoryContext>(true);
            // var context = factory.CreateSqlServer<DexihRepositoryContext>(true);
            var seed = new SeedData();

            var userStore = new UserStore<ApplicationUser>(context);
            var userManager = MockHelpers.TestUserManager(userStore);
            var roleStore = new RoleStore<IdentityRole>(context);
            var roleManager = MockHelpers.TestRoleManager<IdentityRole>(roleStore);
            await seed.UpdateReferenceData(roleManager, userManager);

            return context;
        }

        /// <summary>
        /// Create a basic repository with hub.
        /// </summary>
        /// <returns></returns>
        public async Task<RepositoryManager> CreateRepositoryWithHub()
        {
            var context = await CreateRepositoryEmpty();
            var userStore = new UserStore<ApplicationUser>(context);
            var userManager = MockHelpers.TestUserManager(userStore);
            // instance of the memory cache.
            var memoryCache = new MemoryDistributedCache(new OptionsWrapper<MemoryDistributedCacheOptions>(new MemoryDistributedCacheOptions()));
            var cacheService = new CacheService(memoryCache);

            var database = new RepositoryManager( context, userManager,cacheService, _loggerFactory, null);
            var hub = new DexihHub()
            {
                Name = "test",
                Description = "hub description",
                EncryptionKey = "123"
            };
            var user = await database.FindByEmailAsync("admin@dataexpertsgroup.com");
            var newHub = await database.SaveHub(hub, user, CancellationToken.None);

            Assert.Equal(1, context.DexihHubs.Count());

            return database;
        }

        public async Task<RepositoryManager> CreateRepositoryWithConnections()
        {
            var database = await CreateRepositoryWithHub();
            var hub = await database.DbContext.DexihHubs.SingleAsync(c => c.Name == "test" && c.IsValid);
            var connection = new DexihConnection()
            {
                Name = "source",
                Purpose = EConnectionPurpose.Source,
                Server = "source server",
                ConnectionClassName = "test",
                ConnectionAssemblyName = "asembly",
                HubKey = hub.HubKey
            };

            var saveConnectionResult = await database.SaveConnection(hub.HubKey, connection, CancellationToken.None);

            connection = new DexihConnection()
            {
                Name = "managed",
                Purpose = EConnectionPurpose.Managed,
                Server = "managed server",
                ConnectionClassName = "test",
                ConnectionAssemblyName = "asembly",
                HubKey = hub.HubKey
            };

            saveConnectionResult = await database.SaveConnection(hub.HubKey, connection, CancellationToken.None);

            var dbSourceConnection = database.DbContext.DexihConnections.Single(c => c.Purpose == EConnectionPurpose.Source && c.IsValid);
            Assert.Equal("source", dbSourceConnection.Name);
            Assert.Equal("source server", dbSourceConnection.Server);

            var dbManagedConnection = database.DbContext.DexihConnections.Single(c => c.Purpose == EConnectionPurpose.Managed && c.IsValid);
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
                DexihTableColumns = new[] {
                    new DexihTableColumn() { Name = "key1", LogicalName = "key1", DataType = ETypeCode.String, DeltaType = EDeltaType.NaturalKey },
                    new DexihTableColumn() { Name = "natural key", LogicalName = "natural key", DataType = ETypeCode.Int32, DeltaType = EDeltaType.TrackingField },
                    new DexihTableColumn() { Name = "ignore", LogicalName = "ignore", DataType = ETypeCode.Int32, DeltaType = EDeltaType.IgnoreField }
                },
                ConnectionKey = dbConnection.Key,
				HubKey = dbConnection.HubKey

            };

			await database.SaveTables(dbConnection.HubKey, new[] { table }, true, false, CancellationToken.None);

            return database;
        }



        [Fact]
        public async Task CreateRepository()
        {
	        await using var context = await CreateRepositoryEmpty();
	        Assert.True(context.Users.Any());
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

                //modify the connection and re-save
                dbConnection.Server = "source server 2";
                var saveConnectionResult = await database.SaveConnection(dbConnection.HubKey, dbConnection, CancellationToken.None);
                Assert.NotNull(saveConnectionResult);

                dbConnection = database.DbContext.DexihConnections.Single(c => c.Key == saveConnectionResult.Key && c.IsValid);
                Assert.Equal("source", dbConnection.Name);
                Assert.Equal("source server 2", dbConnection.Server);

                // create a new connection with same name and save it, this should fail.
                var connection = new DexihConnection()
                {
                    Name = "source",
                    Server = "source server 3",
                    ConnectionClassName = "class",
                    ConnectionAssemblyName = "assembly"
                };

                Assert.Throws<AggregateException>(() => database.SaveConnection(dbConnection.HubKey, connection, CancellationToken.None).Result);

				// delete the connection.
				var deleteReturn = await database.DeleteConnections(dbConnection.HubKey, new[] { dbConnection.Key }, CancellationToken.None);
				Assert.NotNull(deleteReturn);

				// check the connection is deleted
				var dbConnection2 = await database.DbContext.DexihConnections.SingleOrDefaultAsync(c => c.Key == dbConnection.Key && c.IsValid);
				Assert.Null(dbConnection2);

				// create a new connection with the same name as the deleted one.  save should succeed
				saveConnectionResult = await database.SaveConnection(dbConnection.HubKey, connection, CancellationToken.None);
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

                // check table is created.
                var dbTable = await database.DbContext.DexihTables.Include(d => d.DexihTableColumns).SingleOrDefaultAsync(c => c.ConnectionKey == dbConnection.Key);
                Assert.NotNull(dbTable);
                Assert.Equal("table1", dbTable.Name);
                Assert.Equal(3, dbTable.DexihTableColumns.Count());


                // update the table, and check update has succeeded.
                dbTable.Name = "updated table";
				var saveResult = await database.SaveTables(dbConnection.HubKey, new[] { dbTable }, true, false, CancellationToken.None);
                Assert.NotNull(saveResult);

                var dbTable2 = await database.DbContext.DexihTables.Include(d => d.DexihTableColumns).SingleOrDefaultAsync(c => c.Key == dbTable.Key && c.IsValid);
                Assert.Equal("updated table", dbTable2.Name);

                // create a new table with the same name.  save should fail
                var duplicateTable = new DexihTable()
                {
	                Name = "updated table",
                    ConnectionKey = dbConnection.Key,
                    BaseTableName = "updated table",
					HubKey = dbConnection.HubKey
                };

                Assert.Throws<AggregateException>(() => database.SaveTables(dbConnection.HubKey, new[] { duplicateTable }, true, false, CancellationToken.None).Result);

				// delete the table.
				var deleteReturn = await database.DeleteTables(dbConnection.HubKey, new[] { dbTable.Key }, CancellationToken.None);
				Assert.NotNull(deleteReturn);

				// check the table is deleted
				dbTable = await database.DbContext.DexihTables.SingleOrDefaultAsync(c => c.Key == dbTable2.Key && c.IsValid);
				Assert.Null(dbTable);

				// create a new table with the same name as the deleted one.  save should succeed
				var saveResult2 = await database.SaveTables(dbConnection.HubKey, new[] {duplicateTable }, true, false, CancellationToken.None);
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

				var saveFileResult = await database.SaveFileFormat(dbConnection.HubKey, fileFormat, CancellationToken.None);

				Assert.NotNull(saveFileResult);

				// lookup the fileformat and check save was correct.
				var fileFormat2 = saveFileResult;
				var fileFormat3 = await database.DbContext.DexihFileFormats.SingleAsync(c => c.Key == fileFormat2.Key && c.IsValid);

				Assert.Equal("new format", fileFormat3.Name);
				Assert.Equal(100, fileFormat3.BufferSize);
				Assert.Equal('-', fileFormat3.Comment);
				Assert.Equal("|", fileFormat3.Delimiter);
				Assert.Equal("the new format", fileFormat3.Description);
				Assert.True(fileFormat3.HasHeaderRecord);
				Assert.True(fileFormat3.IsDefault);

				//test2: update the fileformat, and check update has succeeded.
				fileFormat3.Name = "updated format";
				var saveResult = await database.SaveFileFormat(dbConnection.HubKey, fileFormat3, CancellationToken.None);
				Assert.NotNull(saveResult);

				var fileFormat4 = await database.DbContext.DexihFileFormats.SingleAsync(c => c.Key == fileFormat2.Key && c.IsValid);
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

                Assert.Throws<AggregateException>(() => database.SaveFileFormat(dbConnection.HubKey, duplicateFormat, CancellationToken.None).Result);

				// test4: delete the fileformat.
				var deleteReturn = await database.DeleteFileFormats(dbConnection.HubKey, new[] { fileFormat3.Key }, CancellationToken.None);
				Assert.NotNull(deleteReturn);

				// check the fileformat is deleted
				var fileFormat5 = await database.DbContext.DexihFileFormats.SingleOrDefaultAsync(c => c.Key ==fileFormat3.Key && c.IsValid);
				Assert.Null(fileFormat5);

				//test 5: create a new fileformat with the same name as the deleted one.  save should succeed
				var saveResult2 = await database.SaveFileFormat(dbConnection.HubKey, duplicateFormat, CancellationToken.None);
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
					CleanAction = ECleanAction.DefaultValue,
					DataType = ETypeCode.String,
					CleanValue = "default",
					Description = "validation description",
					InvalidAction = EInvalidAction.Clean,
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
					LookupColumnKey = lookupColumn.Key
				};

				var saveResult = await database.SaveColumnValidation(dbConnection.HubKey, validation, CancellationToken.None);

				Assert.NotNull(saveResult);

				// lookup the validation and check savde was correctly.
				var validation2 = saveResult;
				var validation3 = await database.DbContext.DexihColumnValidations.SingleAsync(c => c.Key == validation2.Key && c.IsValid);

				Assert.Equal("new validation", validation3.Name);
				Assert.Equal(dbConnection.HubKey, validation3.HubKey);
				Assert.True(validation3.AllowDbNull);
				Assert.Equal(ECleanAction.DefaultValue, validation3.CleanAction);
				Assert.Equal(ETypeCode.String, validation3.DataType);
				Assert.Equal("default", validation3.CleanValue);
				Assert.Equal("validation description", validation3.Description);
				Assert.Equal(EInvalidAction.Clean, validation3.InvalidAction);
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
				Assert.Equal(lookupColumn.Key, validation3.LookupColumnKey);

				//test2: update the fileformat, and check update has succeeded.
				validation3.Name = "updated validation";
				var saveResult2 = await database.SaveColumnValidation(dbConnection.HubKey, validation3, CancellationToken.None);
				Assert.NotNull(saveResult2);

				var validation4 = await database.DbContext.DexihColumnValidations.SingleAsync(c => c.Key == validation2.Key && c.IsValid);
				Assert.Equal("updated validation", validation4.Name);

				//test3: create a new table with the same name.  save should fail
				var duplicate = new DexihColumnValidation()
				{
					Name = "updated validation",
					HubKey = dbConnection.HubKey,
					DataType = ETypeCode.String,
				};

                Assert.Throws<AggregateException>(() => database.SaveColumnValidation(dbConnection.HubKey, duplicate, CancellationToken.None).Result);

				// test4: delete the fileformat.
				var deleteReturn = await database.DeleteColumnValidations(dbConnection.HubKey, new[] { validation3.Key }, CancellationToken.None);
				Assert.NotNull(deleteReturn);

				// check the fileformat is deleted
				var validation5 = await database.DbContext.DexihColumnValidations.SingleOrDefaultAsync(c => c.Key == validation3.Key && c.IsValid);
				Assert.Null(validation5);

				//test 5: create a new valiation with the same name as the deleted one.  save should succeed
				var saveResult3 = await database.SaveColumnValidation(dbConnection.HubKey, duplicate, CancellationToken.None);
				Assert.NotNull(saveResult3);
			}
		}

        [Fact]
        public async Task NewDatalink()
        {
            //test 1, create initial table
            using (var repositoryManager = await CreateRepositoryWithSourceTable())
            {
                var dbSourceConnection = await repositoryManager.DbContext.DexihConnections.SingleOrDefaultAsync(c => c.Name == "source" && c.IsValid);
                var dbManagedConnection = await repositoryManager.DbContext.DexihConnections.SingleOrDefaultAsync(c => c.Name == "managed" && c.IsValid);
                var dbSourceTable = await repositoryManager.DbContext.DexihTables.FirstAsync(c => c.Name == "table1" && c.IsValid);

                var newDatalinks = await repositoryManager.NewDatalinks(
                    dbSourceConnection.HubKey, 
                    "test", 
                    EDatalinkType.Stage, 
                    dbManagedConnection.Key, 
                    new[] { dbSourceTable.Key }, 
                    null, 
                    null, 
                    dbManagedConnection.Key,
                    true, 
                    new []{EDeltaType.AutoIncrement, EDeltaType.CreateDate, EDeltaType.UpdateDate, EDeltaType.CreateAuditKey, EDeltaType.IsCurrentField, EDeltaType.UpdateAuditKey, EDeltaType.ValidFromDate, EDeltaType.ValidToDate },
                    new NamingStandards(), CancellationToken.None
                    );

                Assert.Single(newDatalinks);

                var newDatalink = newDatalinks[0];

                Assert.NotNull(newDatalink);
                
                var cacheManager = new CacheManager(dbSourceConnection.HubKey, _loggerFactory.CreateLogger("test"));
                

                var dbDatalink = await cacheManager.GetDatalink(newDatalink.Key, repositoryManager.DbContext);

                // check the datalink properties are set.
                Assert.False(dbDatalink.AddDefaultRow);
                Assert.Equal(dbManagedConnection.Key, dbDatalink.AuditConnectionKey);
                Assert.Equal(EDatalinkType.Stage, dbDatalink.DatalinkType);
                Assert.Equal(1, dbDatalink.DexihDatalinkTransforms.Count);
                Assert.Equal(dbSourceConnection.HubKey, dbDatalink.HubKey);
                Assert.False(dbDatalink.IsShared);
                Assert.True(dbDatalink.IsValid);
                Assert.False(dbDatalink.IsQuery);
                Assert.False(dbDatalink.RollbackOnFail);
                Assert.Null(dbDatalink.SourceDatalinkTable.SourceDatalinkKey);
                Assert.Equal(dbSourceTable.Key, dbDatalink.SourceDatalinkTable.SourceTableKey);
                Assert.Equal(ESourceType.Table, dbDatalink.SourceDatalinkTable.SourceType);
                Assert.True(dbDatalink.DexihDatalinkTargets.Count > 0);
                Assert.Equal(EUpdateStrategy.AppendUpdateDelete, dbDatalink.UpdateStrategy);

                // check target table created properly.
                var dbTable = await repositoryManager.GetTable(newDatalink.HubKey, (long)newDatalink.DexihDatalinkTargets.First().TableKey, true, CancellationToken.None);

                Assert.Equal("stgtable1", dbTable.Name);
                Assert.Equal("table1", dbTable.BaseTableName);

                //should be no sourcesurrgate key, as source table has no surrogate key
                Assert.Null(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == EDeltaType.SourceSurrogateKey));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == EDeltaType.AutoIncrement));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == EDeltaType.CreateAuditKey));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == EDeltaType.UpdateAuditKey));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == EDeltaType.CreateDate));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == EDeltaType.UpdateDate));
                Assert.NotNull(dbTable.DexihTableColumns.SingleOrDefault(c => c.DeltaType == EDeltaType.IsCurrentField));

                // check target columns were created
                foreach(var col in dbSourceTable.DexihTableColumns)
                {
	                // var name = dbManagedConnection.DatabaseType.RemoveUnsupportedCharacaters(col.Name);
	                var name = col.Name;
                    if(col.DeltaType == EDeltaType.IgnoreField)
                    {
                        Assert.Null(dbTable.DexihTableColumns.SingleOrDefault(c => c.Name == name));
                    }
                    else
                    {
                        var targetColumn = dbTable.DexihTableColumns.SingleOrDefault(c => c.Name == name);
                        Assert.NotNull(targetColumn);

                        //if the target column name is cleaned and not the same, then a mapping should be created.
                        if(name != col.Name)
                        {
                            var datalinkTransform = dbDatalink.DexihDatalinkTransforms.SingleOrDefault(c => c.TransformType == ETransformType.Mapping);
                            Assert.NotNull(datalinkTransform);

                            var mapping = datalinkTransform.DexihDatalinkTransformItems.SingleOrDefault(c => c.SourceDatalinkColumn.Name == col.Name);
                            Assert.NotNull(mapping);
                            Assert.Equal(targetColumn.Name, mapping.TargetDatalinkColumn.Name);
                        }
                    }
                }

                dbDatalink.Name = "new name";
                await repositoryManager.SaveDatalinks(dbSourceConnection.HubKey, new[] { dbDatalink }, false, CancellationToken.None);
                var dbDatalink2 = await cacheManager.GetDatalink(dbDatalink.Key, repositoryManager.DbContext);
                Assert.Equal("new name", dbDatalink2.Name);

                var dbDatalink3 = new DexihDatalink();
                dbDatalink2.CopyProperties(dbDatalink3);
                dbDatalink3.Name = "newer name";
                await repositoryManager.SaveDatalinks(dbSourceConnection.HubKey, new[] { dbDatalink3 }, false, CancellationToken.None);
                var dbDatalink4 = await cacheManager.GetDatalink(dbDatalink.Key, repositoryManager.DbContext);
                Assert.Equal("newer name", dbDatalink4.Name);

            }
        }
    }
}
