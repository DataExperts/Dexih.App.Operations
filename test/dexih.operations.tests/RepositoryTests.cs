using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using dexih.functions;
using dexih.repository;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Identity.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Xunit;

namespace dexih.operations.tests
{
    public class RepositoryManagerTests
    {
        public RepositoryManagerTests()
        {
            // create an in memory database for testing
            var options = new DbContextOptionsBuilder<DexihRepositoryContext>()
                .UseInMemoryDatabase("dexih_repository")
                .Options;
            
            var repositoryContext = new DexihRepositoryContext(options);
            repositoryContext.Database.EnsureCreated();
            

            // instance of the memory cache.
            var memoryCache = new MemoryDistributedCache(new OptionsWrapper<MemoryDistributedCacheOptions>(new MemoryDistributedCacheOptions()));

            var userStore = new UserStore<ApplicationUser>(repositoryContext);
            var roleStore = new RoleStore<IdentityRole>(repositoryContext);
            var userManager = MockHelpers.TestUserManager(userStore);
            var roleManager = MockHelpers.TestRoleManager(roleStore);

            var seedData = new SeedData();
            seedData.UpdateReferenceData(repositoryContext, roleManager, userManager).Wait();

            _repositoryManager = new RepositoryManager("key", repositoryContext, userManager, memoryCache);
                        
        }

        private readonly RepositoryManager _repositoryManager;
       
        private async Task<DexihHub> CreateHub()
        {
            var user = new ApplicationUser()
            {
                Email = "admin@dataexpertsgroup.com",
                Id = "123"
            };
            
            // use a guid to make the hub name unique
            var guid = Guid.NewGuid().ToString();
            var hub = new DexihHub()
            {
                Name = guid,
                Description = "Description",
            };

            return await _repositoryManager.SaveHub(hub, user);
        }

        private async Task<ApplicationUser> CreateUser(ApplicationUser.EUserRole userRole)
        {
            var email = Guid.NewGuid() + "@dataexpertsgroup.com";
            var appUser = new ApplicationUser()
            {
                Email = email,
                FirstName = "First",
                LastName = "Last",
                HubQuota = 5,
                InviteQuota = 6,
                UserName = "user",
                UserRole = userRole
            };

            await _repositoryManager.CreateUserAsync(appUser);
            return appUser;
        }

        [Fact]
        public async Task Add_User()
        {
            var appUser = await CreateUser(ApplicationUser.EUserRole.Manager);

            var retrievedUser = await _repositoryManager.GetUserFromEmail(appUser.Email);
            Assert.Equal(appUser.FirstName, retrievedUser.FirstName);
            Assert.Equal(appUser.LastName, retrievedUser.LastName);
            Assert.Equal(appUser.Email, retrievedUser.Email);
            Assert.Equal(appUser.UserName, retrievedUser.UserName);
            Assert.Equal(appUser.HubQuota, retrievedUser.HubQuota);
            Assert.Equal(appUser.InviteQuota, retrievedUser.InviteQuota);
            Assert.Equal(ApplicationUser.EUserRole.Manager, appUser.UserRole);
        }
        
        [Fact]
        public async Task Add_Delete_Hub()
        {
            var hub = await CreateHub();
            Assert.True(hub.HubKey > 0);
            
            // retrieve hub
            var retrievedHub = await _repositoryManager.GetHub(hub.HubKey);
            
            Assert.Equal(hub.Name, retrievedHub.Name);
            Assert.Equal(hub.Description, retrievedHub.Description);

            // set a user to owner access 
            var user = await CreateUser(ApplicationUser.EUserRole.Manager);
            await _repositoryManager.HubSetUserPermissions(hub.HubKey, new[] {user.Id}, DexihHubUser.EPermission.Owner);

            var adminUser = await CreateUser(ApplicationUser.EUserRole.Administrator);

            // get users for the hub
            var hubUsers = await _repositoryManager.GetHubUsers(hub.HubKey);
            Assert.NotNull(hubUsers.SingleOrDefault(c => c.Email == user.Email));

            // get hubs for the user
            var hubs = await _repositoryManager.GetUserHubs(user);
            Assert.NotNull(hubs.SingleOrDefault(c=>c.HubKey == hub.HubKey));
            
            // load the hub into the cache
            var hubCache = await _repositoryManager.GetHub(hub.HubKey);
            Assert.Equal(hub.Name, hubCache.Name);
            
            // check admin also can get the hub
            hubs = await _repositoryManager.GetUserHubs(adminUser);
            Assert.NotNull(hubs.SingleOrDefault(c=>c.HubKey == hub.HubKey));
            
            // remote permission for the user
            await _repositoryManager.HubSetUserPermissions(hub.HubKey, new[] {user.Id}, DexihHubUser.EPermission.None);

            // get users for the hub
            hubUsers = await _repositoryManager.GetHubUsers(hub.HubKey);
            Assert.NotNull(hubUsers.SingleOrDefault(c => c.Email == user.Email && c.Permission == DexihHubUser.EPermission.None));

            // get hubs for the user
            hubs = await _repositoryManager.GetUserHubs(user);
            Assert.Null(hubs.SingleOrDefault(c=>c.HubKey == hub.HubKey));

            // delete hub (this will fail as permissions have been set to none.
            await Assert.ThrowsAsync<RepositoryManagerException>(async () => await _repositoryManager.DeleteHubs(user, new[] {hub.HubKey}));

            // set users permission back to owner and then delete.
            await _repositoryManager.HubSetUserPermissions(hub.HubKey, new[] {user.Id}, DexihHubUser.EPermission.Owner);
            await _repositoryManager.DeleteHubs(user, new[] {hub.HubKey});
                
            // check hub is gone
            hubs = await _repositoryManager.GetUserHubs(adminUser);
            Assert.Null(hubs.SingleOrDefault(c=>c.HubKey == hub.HubKey));
            
            // should raise exception loading the hub cache
            await Assert.ThrowsAsync<CacheManagerException>(async () => await _repositoryManager.GetHub(hub.HubKey));
        }

        [Fact]
        public async Task Save_Connection_and_Tables()
        {
            var hub = await CreateHub();

            // create initial connection
            var connection = new DexihConnection()
            {
                Name = "connection",
                Description = "description",
                ConnectionAssemblyName = "assembly",
                Server = "server",
                Password = "password",
            };
            await _repositoryManager.SaveConnection(hub.HubKey, connection);
            
            Assert.True(connection.Key > 0);
            
            // add table to the connection
            var table = new DexihTable()
            {
                Name = "table",
                Description = "description",
                ConnectionKey = connection.Key,
                DexihTableColumns = new List<DexihTableColumn>()
                {
                    new DexihTableColumn() {Name = "column1", DataType = DataType.ETypeCode.Int32, DeltaType = TableColumn.EDeltaType.TrackingField},
                    new DexihTableColumn()
                    {
                        Name = "column2", DataType = DataType.ETypeCode.Node, DeltaType = TableColumn.EDeltaType.TrackingField,
                        ChildColumns = new List<DexihTableColumn> ()
                        {
                            new DexihTableColumn() {Name = "childColumn1", DataType = DataType.ETypeCode.Int32, DeltaType = TableColumn.EDeltaType.TrackingField},
                            new DexihTableColumn()
                            {
                                Name = "childColumn2", DataType = DataType.ETypeCode.Node, DeltaType = TableColumn.EDeltaType.TrackingField,
                                ChildColumns = new List<DexihTableColumn> ()
                                {
                                    new DexihTableColumn() {Name = "grandChildColumn1", DataType = DataType.ETypeCode.Int32, DeltaType = TableColumn.EDeltaType.TrackingField},
                                    new DexihTableColumn() {Name = "grandChildColumn2", DataType = DataType.ETypeCode.Int32, DeltaType = TableColumn.EDeltaType.TrackingField},
                                }
                            },
                        }
                        
                    },
                }
            };

            var savedTable = await _repositoryManager.SaveTable(hub.HubKey, table, true);

            Assert.True(savedTable.Key > 0);
            
            // retrieve the connection and the table
            var retrievedConnection = await _repositoryManager.GetConnection(hub.HubKey, connection.Key, true);
            Assert.Equal(connection.Key, retrievedConnection.Key);
            Assert.Equal(connection.ConnectionAssemblyName, retrievedConnection.ConnectionAssemblyName);
            Assert.Equal(connection.Server, retrievedConnection.Server);
            Assert.Equal(connection, retrievedConnection);

            
            var retrievedTable = hub.DexihTables.Single(c => c.Key == savedTable.Key);
            retrievedTable = retrievedTable.CloneProperties<DexihTable>(); //clone the table to avoid conflicts with MemoryDatabase.
            var columns = retrievedTable.DexihTableColumns.ToArray();

            Assert.Equal(table.Name, retrievedTable.Name);
            Assert.Equal(table.Description, retrievedTable.Description);
            Assert.Equal(2, table.DexihTableColumns.Count);
            Assert.Equal("column1", columns[0].Name);
            Assert.Equal("column2", columns[1].Name);

            var childColumns = columns[1].ChildColumns.ToArray();
            Assert.Equal(2, childColumns.Length);
            Assert.Equal(2, childColumns[1].ChildColumns.Count);
            
            // update the table name
            retrievedTable.Name = "table updated";
            // update a column
            columns[0].Name = "column1 updated";
            // delete a column
            retrievedTable.DexihTableColumns.Remove(columns[1]);
            // add a column
            retrievedTable.DexihTableColumns.Add(new DexihTableColumn() {Name = "column3", DataType = DataType.ETypeCode.Int32, DeltaType = TableColumn.EDeltaType.TrackingField});
            
            var savedTable2 = await _repositoryManager.SaveTable(hub.HubKey, retrievedTable, true);
            var columns2 = savedTable2.DexihTableColumns.Where(c=>c.IsValid).ToArray();
            var retrievedTable2 = await _repositoryManager.GetTable(hub.HubKey, savedTable2.Key, true);
            Assert.Equal(savedTable2.Name, retrievedTable2.Name);
            Assert.Equal(2, columns2.Length);
            Assert.Equal("column1 updated", columns2[0].Name);
            Assert.Equal("column3", columns2[1].Name);
            
            // delete the table
            await _repositoryManager.DeleteTables(hub.HubKey, new[] {savedTable.Key});
            
            // confirm table deleted
            await Assert.ThrowsAsync<RepositoryManagerException>(async () => await _repositoryManager.GetTable(hub.HubKey, savedTable.Key, true));

            // retrieve the connection with tables, and confirm table not retrieved.
            retrievedConnection = await _repositoryManager.GetConnection(hub.HubKey, connection.Key, true);
            Assert.Equal(0, hub.DexihTables.Count(c => c.IsValid));
        }
    }
}