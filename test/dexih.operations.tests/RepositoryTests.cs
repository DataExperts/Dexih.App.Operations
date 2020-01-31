using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using dexih.functions;
using dexih.repository;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;
using Xunit;

namespace dexih.operations.tests
{
    public class RepositoryManagerTests
    {
        public RepositoryManagerTests()
        {
            _repositoryManager = MockHelpers.CreateRepositoryManager();
        }

        private readonly RepositoryManager _repositoryManager;
       
        private async Task<DexihHub> CreateHub()
        {
            var user =  await _repositoryManager.FindByEmailAsync("admin@dataexpertsgroup.com");
            user = await _repositoryManager.GetUserAsync(user.Id, CancellationToken.None);
            // use a guid to make the hub name unique
            var guid = Guid.NewGuid().ToString();
            var hub = new DexihHub()
            {
                Name = guid,
                Description = "Description",
            };

            return await _repositoryManager.SaveHub(hub, user, CancellationToken.None);
        }
        private async Task<ApplicationUser> CreateUser(EUserRole userRole)
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

            await _repositoryManager.CreateUserAsync(appUser, null, CancellationToken.None);
            return appUser;
        }

        [Fact]
        public async Task Add_User()
        {
            var appUser = await CreateUser(EUserRole.Manager);

            var retrievedUser = await _repositoryManager.GetUserFromLoginAsync(appUser.Email, CancellationToken.None);
            Assert.Equal(appUser.FirstName, retrievedUser.FirstName);
            Assert.Equal(appUser.LastName, retrievedUser.LastName);
            Assert.Equal(appUser.Email, retrievedUser.Email);
            Assert.Equal(appUser.UserName, retrievedUser.UserName);
            Assert.Equal(appUser.HubQuota, retrievedUser.HubQuota);
            Assert.Equal(appUser.InviteQuota, retrievedUser.InviteQuota);
            Assert.Equal(EUserRole.Manager, appUser.UserRole);
        }
        
        [Fact]
        public async Task Add_Delete_Hub()
        {
            var hub = await CreateHub();
            Assert.True(hub.HubKey > 0);
            
            // retrieve hub
            var retrievedHub = await _repositoryManager.GetHub(hub.HubKey, CancellationToken.None);
            
            Assert.Equal(hub.Name, retrievedHub.Name);
            Assert.Equal(hub.Description, retrievedHub.Description);

            // set a user to owner access 
            var user = await CreateUser(EUserRole.Manager);
            await _repositoryManager.HubSetUserPermissions(hub.HubKey, new[] {user.Id}, EPermission.Owner, CancellationToken.None);

            var adminUser = await CreateUser(EUserRole.Administrator);

            // get users for the hub
            var hubUsers = await _repositoryManager.GetHubUsers(hub.HubKey, CancellationToken.None);
            Assert.NotNull(hubUsers.SingleOrDefault(c => c.UserName == user.UserName));

            // get hubs for the user
            var hubs = await _repositoryManager.GetUserHubs(user, CancellationToken.None);
            Assert.NotNull(hubs.SingleOrDefault(c=>c.HubKey == hub.HubKey));
            
            // load the hub into the cache
            var hubCache = await _repositoryManager.GetHub(hub.HubKey, CancellationToken.None);
            Assert.Equal(hub.Name, hubCache.Name);
            
            // check admin also can get the hub
            hubs = await _repositoryManager.GetUserHubs(adminUser, CancellationToken.None);
            Assert.NotNull(hubs.SingleOrDefault(c=>c.HubKey == hub.HubKey));
            
            // remote permission for the user
            await _repositoryManager.HubSetUserPermissions(hub.HubKey, new[] {user.Id}, EPermission.None, CancellationToken.None);

            // get users for the hub
            hubUsers = await _repositoryManager.GetHubUsers(hub.HubKey, CancellationToken.None);
            Assert.NotNull(hubUsers.SingleOrDefault(c => c.UserName == user.UserName && c.Permission == EPermission.None));

            // get hubs for the user
            hubs = await _repositoryManager.GetUserHubs(user, CancellationToken.None);
            Assert.Null(hubs.SingleOrDefault(c=>c.HubKey == hub.HubKey));

            // delete hub (this will fail as permissions have been set to none.
            await Assert.ThrowsAsync<RepositoryManagerException>(async () => await _repositoryManager.DeleteHubs(user, new[] {hub.HubKey}, CancellationToken.None));

            // set users permission back to owner and then delete.
            await _repositoryManager.HubSetUserPermissions(hub.HubKey, new[] {user.Id}, EPermission.Owner, CancellationToken.None);
            await _repositoryManager.DeleteHubs(user, new[] {hub.HubKey}, CancellationToken.None);
                
            // check hub is gone
            hubs = await _repositoryManager.GetUserHubs(adminUser, CancellationToken.None);
            Assert.Null(hubs.SingleOrDefault(c=>c.HubKey == hub.HubKey));
            
            // should raise exception loading the hub cache
            await Assert.ThrowsAsync<CacheManagerException>(async () => await _repositoryManager.GetHub(hub.HubKey, CancellationToken.None));
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
            await _repositoryManager.SaveConnection(hub.HubKey, connection, CancellationToken.None);
            
            Assert.True(connection.Key > 0);
            
            // add table to the connection
            var table = new DexihTable()
            {
                Name = "table",
                Description = "description",
                ConnectionKey = connection.Key,
                DexihTableColumns = new List<DexihTableColumn>()
                {
                    new DexihTableColumn() {Name = "column1", DataType = ETypeCode.Int32, DeltaType = EDeltaType.TrackingField},
                    new DexihTableColumn()
                    {
                        Name = "column2", DataType = ETypeCode.Node, DeltaType = EDeltaType.TrackingField,
                        ChildColumns = new List<DexihTableColumn> ()
                        {
                            new DexihTableColumn() {Name = "childColumn1", DataType = ETypeCode.Int32, DeltaType = EDeltaType.TrackingField},
                            new DexihTableColumn()
                            {
                                Name = "childColumn2", DataType = ETypeCode.Node, DeltaType = EDeltaType.TrackingField,
                                ChildColumns = new List<DexihTableColumn> ()
                                {
                                    new DexihTableColumn() {Name = "grandChildColumn1", DataType = ETypeCode.Int32, DeltaType = EDeltaType.TrackingField},
                                    new DexihTableColumn() {Name = "grandChildColumn2", DataType = ETypeCode.Int32, DeltaType = EDeltaType.TrackingField},
                                }
                            },
                        }
                        
                    },
                }
            };

            var savedTable = await _repositoryManager.SaveTable(hub.HubKey, table, true, false, CancellationToken.None);

            Assert.True(savedTable.Key > 0);
            
            // retrieve the connection and the table
            var retrievedConnection = await _repositoryManager.GetConnection(hub.HubKey, connection.Key, true, CancellationToken.None);
            Assert.Equal(connection.Key, retrievedConnection.Key);
            Assert.Equal(connection.ConnectionAssemblyName, retrievedConnection.ConnectionAssemblyName);
            Assert.Equal(connection.Server, retrievedConnection.Server);
            Assert.Equal(connection, retrievedConnection);

            
            var retrievedTable = await _repositoryManager.GetTable(hub.HubKey, savedTable.Key, true, CancellationToken.None);
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
            retrievedTable.DexihTableColumns.Add(new DexihTableColumn() {Name = "column3", DataType = ETypeCode.Int32, DeltaType = EDeltaType.TrackingField});
            
            var savedTable2 = await _repositoryManager.SaveTable(hub.HubKey, retrievedTable, true, false, CancellationToken.None);
            var columns2 = savedTable2.DexihTableColumns.Where(c=>c.IsValid).ToArray();
            var retrievedTable2 = await _repositoryManager.GetTable(hub.HubKey, savedTable2.Key, true, CancellationToken.None);
            Assert.Equal(savedTable2.Name, retrievedTable2.Name);
            Assert.Equal(2, columns2.Length);
            Assert.Equal("column1 updated", columns2[0].Name);
            Assert.Equal("column3", columns2[1].Name);
            
            // delete the table
            await _repositoryManager.DeleteTables(hub.HubKey, new[] {savedTable.Key}, CancellationToken.None);
            
            // confirm table deleted
            await Assert.ThrowsAsync<RepositoryManagerException>(async () => await _repositoryManager.GetTable(hub.HubKey, savedTable.Key, true, CancellationToken.None));

            // retrieve the connection with tables, and confirm table not retrieved.
            retrievedConnection = await _repositoryManager.GetConnection(hub.HubKey, connection.Key, true, CancellationToken.None);
            Assert.Equal(0, hub.DexihTables.Count(c => c.IsValid));
        }
    }
}