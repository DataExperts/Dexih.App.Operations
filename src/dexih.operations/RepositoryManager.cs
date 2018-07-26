using dexih.functions;
using dexih.repository;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static dexih.functions.TableColumn;
using Microsoft.Extensions.Logging;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.CopyProperties;
using System.Threading;
using dexih.transforms;
using dexih.transforms.Transforms;
using Microsoft.Extensions.Caching.Memory;

namespace dexih.operations
{

	
	/// <summary>
	/// Provides an interface to retrieve and save to the database repository.
	/// </summary>
	public class RepositoryManager : IDisposable
	{
		private string SystemEncryptionKey { get; set; }
		private ILogger Logger { get; set; }

		public DexihRepositoryContext DbContext { get; set; }
		public IMemoryCache MemoryCache { get; set; }

		public readonly Func<Import, Task> HubChange;

		private long _internalConnectionKey = 0;

		public RepositoryManager(
			 string systemEncryptionKey, 
             DexihRepositoryContext dbContext,
			 IMemoryCache memoryCache,
             ILoggerFactory loggerFactory,
			 Func<Import, Task> hubChange
            )
		{
			SystemEncryptionKey = systemEncryptionKey;
			DbContext = dbContext;
			MemoryCache = memoryCache;
			Logger = loggerFactory.CreateLogger("RepositoryManager");
			HubChange = hubChange;

			if (memoryCache == null)
			{
				memoryCache = new MemoryCache(new MemoryCacheOptions());
			}

		}

		public RepositoryManager(string systemEncryptionKey, 
			DexihRepositoryContext dbContext,
			IMemoryCache memoryCache
		)
		{
			SystemEncryptionKey = systemEncryptionKey;
			DbContext = dbContext;
			MemoryCache = memoryCache;

			if (memoryCache == null)
			{
				memoryCache = new MemoryCache(new MemoryCacheOptions());
			}
		}
		
		public void Dispose()
		{
			DbContext.Dispose();
		}

		#region Hub Functions

		public void ResetUserCache(string userId)
		{
			MemoryCache.Remove($"USERHUBS-{userId}");
		}

		public void ResetHubCache(long hubKey)
		{
			MemoryCache.Remove($"HUB-{hubKey}");
			MemoryCache.Remove($"HUBUSERIDS-{hubKey}");
			MemoryCache.Remove($"HUBUSERS-{hubKey}");

		}
		
		/// <summary>
		/// Retrieves an array containing the hub and all depdendencies along with any dependent objects
		/// </summary>
		/// <param name="hubKey"></param>
		/// <returns></returns>
		public Task<DexihHub> GetHub(long hubKey)
		{
			var hubReturn = MemoryCache.GetOrCreateAsync($"HUB-{hubKey}", async entry =>
			{
				entry.SlidingExpiration = TimeSpan.FromHours(1);
				var cache = new CacheManager(hubKey, await GetHubEncryptionKey(hubKey));
				var hub = await cache.LoadHub(DbContext);
				return hub;
			});

			return hubReturn;
		}
		
		public async Task<IEnumerable<DexihHubVariable>> GetHubVariables(long hubKey)
		{
			var hubVariables = await DbContext.DexihHubVariable.Where(c => c.HubKey == hubKey && c.IsValid).ToArrayAsync();
			return hubVariables;
		}

		public async Task<DexihHubUser.EPermission> GetHubUserPermission(long hubKey, string userId)
		{
			var hubUser = await DbContext.DexihHubUser.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == userId && c.IsValid);
			if (hubUser == null)
			{
				return DexihHubUser.EPermission.None;
			}
			else
			{
				return hubUser.Permission;
			}
		}

		/// <summary>
		/// Get hubs which the user is either authorized (Owner, User, FullReader) or an admin.
		/// </summary>
		/// <param name="userId"></param>
		/// <param name="isAdmin"></param>
		/// <returns></returns>
		public Task<DexihHub[]> GetUserHubs(string userId, bool isAdmin)
		{
			return MemoryCache.GetOrCreateAsync($"USERHUBS-{userId}", async entry =>
			{
				entry.SlidingExpiration = TimeSpan.FromMinutes(1);
				
				if (isAdmin)
				{
					var hubs = await DbContext.DexihHubs
						.Include(c => c.DexihHubUsers)
						.Include(c => c.DexihRemoteAgentHubs)
						.Where(c => !c.IsInternal && c.IsValid)
						.ToArrayAsync();
					return hubs;
				}
				else
				{
					var hubKeys = await DbContext.DexihHubUser
						.Where(c => c.UserId == userId && c.Permission <= DexihHubUser.EPermission.FullReader && c.IsValid)
						.Select(c => c.HubKey).ToArrayAsync();
					
					var hubs = await DbContext.DexihHubs
						.Include(c => c.DexihHubUsers)
						.Include(c => c.DexihRemoteAgentHubs)
						.Where(c => hubKeys.Contains(c.HubKey) && !c.IsInternal && c.IsValid).ToArrayAsync();
					return hubs;
				}
			});
		}
		
		/// <summary>
		/// Gets a list of hubs the user can access shared data in.
		/// </summary>
		/// <param name="userId"></param>
		/// <param name="isAdmin"></param>
		/// <returns></returns>
		public async Task<DexihHub[]> GetSharedHubs(string userId, bool isAdmin)
		{
			// determine the hubs which can be shared data can be accessed from
			if (isAdmin)
			{
				// admin user has access to all hubs
				return await DbContext.DexihHubs.Where(c => !c.IsInternal && c.IsValid).ToArrayAsync();
			}
			else
			{
				if (string.IsNullOrEmpty(userId))
				{
					// no user can only see public hubs
					return await DbContext.DexihHubs.Where(c => c.SharedAccess == DexihHub.ESharedAccess.Public && !c.IsInternal && c.IsValid).ToArrayAsync();
				}
				else
				{
					// all hubs the user has reader access to.
					var readerHubKeys = await DbContext.DexihHubUser.Where(c => c.UserId == userId && c.Permission >= DexihHubUser.EPermission.PublishReader && c.IsValid).Select(c=>c.HubKey).ToArrayAsync();
					
					// all hubs the user has reader access to, or are public
					return await DbContext.DexihHubs.Where(c => 
						(!c.IsInternal && c.IsValid) &&
						(
							c.SharedAccess == DexihHub.ESharedAccess.Public ||
							c.SharedAccess == DexihHub.ESharedAccess.Registered ||
							readerHubKeys.Contains(c.HubKey)
						)
						).ToArrayAsync();
				}
			}
		}

		/// <summary>
		/// Checks is the user can access shared objects in the hub.
		/// </summary>
		/// <param name="userId"></param>
		/// <param name="isAdmin"></param>
		/// <param name="hubKey"></param>
		/// <returns></returns>
		public async Task<bool> CanAccessSharedObjects(string userId, bool isAdmin, long hubKey)
		{
			// determine the hubs which can be shared data can be accessed from
			if (isAdmin)
			{
				return true;
			}
			else
			{
				if (string.IsNullOrEmpty(userId))
				{
					// no user can only see public hubs
					var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.SharedAccess == DexihHub.ESharedAccess.Public && !c.IsInternal && c.IsValid);
					return hub != null;
				}
				else
				{
					// all hubs the user has reader access to.
					var readerHubKey = await DbContext.DexihHubUser.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == userId && c.Permission >= DexihHubUser.EPermission.PublishReader && c.IsValid);

					if (readerHubKey != null)
					{
						return true;
					}
					
					// all hubs other public/shared hubs
					var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => 
						c.HubKey == hubKey &&
						(!c.IsInternal && c.IsValid) &&
						(
							c.SharedAccess == DexihHub.ESharedAccess.Public ||
							c.SharedAccess == DexihHub.ESharedAccess.Registered 
						)
					);
					return hub != null;
				}
			}
		}

		/// <summary>
		/// Gets a list of all the shared datalinks/table available to the user.
		/// </summary>
		/// <param name="userId">User.Id</param>
		/// <param name="isAdmin">Is user admin</param>
		/// <param name="searchString">Search string to restrict</param>
		/// <param name="hubKeys">HubKeys to restrict search to (null/empty will use all available hubs)</param>
		/// <param name="maxResults">Maximum results to return (0 for all).</param>
		/// <returns></returns>
		/// <exception cref="RepositoryException"></exception>
		public async Task<IEnumerable<SharedData>> GetSharedDataIndex(string userId, bool isAdmin, string searchString, long[] hubKeys, int maxResults = 0)
		{
			var availableHubs = await GetSharedHubs(userId, isAdmin);

			// check user has access to all the requested hub keys
			if (hubKeys != null && hubKeys.Length > 0)
			{
				foreach (var hubKey in hubKeys)
				{
					if (!availableHubs.Select(c=>c.HubKey).Contains(hubKey))
					{
						throw new RepositoryException($"The user does not have access to the hub with the key ${hubKey}");
					}
				}
			}
			else
			{
				// if no hubkeys specified, then user all available.
				hubKeys = availableHubs.Select(c => c.HubKey).ToArray();
			}

			var sharedData = new List<SharedData>();
			var counter = 0;
			var search = searchString?.ToLower();
			var noSearch = string.IsNullOrEmpty(search);

			// load shared objects for each available hub
			foreach (var hubKey in hubKeys)
			{
				var hub = await GetHub(hubKey);
				foreach (var connection in hub.DexihConnections)
				{
					foreach (var table in connection.DexihTables.Where(c => c.IsShared && ( noSearch || c.Name.ToLower().Contains(search))))
					{
						sharedData.Add(new SharedData()
						{
							HubKey = hub.HubKey,
							HubName = hub.Name,
							ObjectKey = table.TableKey,
							ObjectType = SharedData.EObjectType.Table,
							Name = table.Name,
							LogicalName = table.LogicalName,
							Description = table.Description,
							UpdateDate = table.UpdateDate,
							InputColumns = table.DexihTableColumns.Where(c => c.IsInput).Select(c => (DexihColumnBase) c)
								.ToArray(),
							OutputColumns = table.DexihTableColumns.Select(c => (DexihColumnBase) c).ToArray()
						});

						if (counter++ > maxResults)
						{
							return sharedData;
						}
					}
				}

				foreach (var datalink in hub.DexihDatalinks.Where(c => c.IsShared && ( noSearch || c.Name.ToLower().Contains(search))))
				{
					sharedData.Add(new SharedData()
					{
						HubKey = datalink.HubKey,
						HubName = hub.Name,
						ObjectKey = datalink.DatalinkKey,
						ObjectType = SharedData.EObjectType.Datalink,
						Name =  datalink.Name,
						LogicalName =  datalink.Name,
						Description =  datalink.Description,
						UpdateDate =  datalink.UpdateDate,
						InputColumns = datalink.SourceDatalinkTable?.DexihDatalinkColumns?.Select(c => (DexihColumnBase)c).Where(c => c.IsInput).ToArray(),
						OutputColumns = datalink.GetOutputTable().DexihDatalinkColumns.Select(c => (DexihColumnBase)c).ToArray()
					});

					if (counter++ > maxResults)
					{
						return sharedData;
					}
				}
			}

			return sharedData;


		}
		
		/// <summary>
		/// Saves changes in the dbContext, and raises event to report these back to the client.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
        public async Task SaveHubChangesAsync(long hubKey, CancellationToken cancellationToken = default(CancellationToken))
        {
            
            var entities = DbContext.ChangeTracker.Entries().Where(x => (
	            x.State == EntityState.Added || 
	            x.State == EntityState.Modified ||
	            x.State == EntityState.Deleted));

	        var import = new Import(hubKey);

	        foreach (var entity in entities)
	        {
		        var item = entity.Entity;

		        // remote unwanted entries.
//		        if (item is DexihSetting)
//		        {
//			        entity.State = EntityState.Unchanged;
//			        continue;
//		        }

		        // other entries.  If the key value <=0 modify the state to added, otherwise modify existing entity.
		        var properties = item.GetType().GetProperties();

		        var importAction = EImportAction.Skip;
		        
		        switch (entity.State)
		        {
			        case EntityState.Added:
				        importAction = EImportAction.New;
				        break;
			        case EntityState.Deleted:
				        importAction = EImportAction.Delete;
				        break;
			        case EntityState.Modified:
				        importAction = EImportAction.Replace;
				        break;
		        }
		        		        
		        foreach (var property in properties)
		        {
			        foreach (var attrib in property.GetCustomAttributes(true))
			        {
				        if (attrib is CopyCollectionKeyAttribute)
				        {
					        var value = (long) property.GetValue(item);
					        entity.State = value <= 0 ? EntityState.Added : EntityState.Modified;
				        }

				        // if the isvalid = false, then set the import action to delete.
				        if (attrib is CopyIsValidAttribute)
				        {
					        var value = (bool) property.GetValue(item);
					        if (value == false)
					        {
						        importAction = EImportAction.Delete;
					        }
				        }
			        }
		        }

		        // add the items to the change list, with will be broadcast back to the client.
		        import.Add(item, importAction);
	        }

            await DbContext.SaveHub(hubKey, true, cancellationToken);
	        
	        if (import.Any())
	        {
		        // TODO: Better for performance to merge changes into cache, but leaving for now as do not want to risk out of sync scenarios
		        ResetHubCache(hubKey);
		        
		        // raise event to send changes back to client.
		        if (HubChange != null)
		        {
			        await HubChange.Invoke(import);
		        }
	        }
        }

		/// <summary>
		/// Gets a list of userIds which have access to the specified hubKey.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <returns></returns>
		public Task<string[]> GetHubUserIds(long hubKey)
		{
			return MemoryCache.GetOrCreateAsync($"HUBUSERIDS-{hubKey}", async entry =>
			{
				entry.SlidingExpiration = TimeSpan.FromMinutes(1);

				try
				{
					var adminId = await DbContext.Roles
						.SingleAsync(c => c.Name == "ADMINISTRATOR");
					
					var adminUsers = await DbContext.UserRoles
						.Where(c => c.RoleId == adminId.Id)
						.Select(c => c.UserId)
						.ToArrayAsync();
					
					var hubUserIds = await DbContext.DexihHubUser
						.Where(c => !adminUsers.Contains(c.UserId) && c.HubKey == hubKey && c.IsValid)
						.Select(c => c.UserId).ToListAsync();
					
					hubUserIds.AddRange(adminUsers);

					var hubUserNames = await DbContext.Users
						.Where(c => hubUserIds.Contains(c.Id))
						.Select(c => c.Id).ToArrayAsync();

					return hubUserNames;
				}
				catch (Exception ex)
				{
					throw new RepositoryManagerException($"Error getting hub user ids.  {ex.Message}", ex);
				}
			});
		}

        /// <summary>
        /// Gets a list of users with names and permission to hub.
        /// </summary>
        /// <param name="hubKey"></param>
        /// <returns></returns>
        public Task<List<HubUser>> GetHubUsers(long hubKey)
        {
            try
            {
	            var returnList = MemoryCache.GetOrCreateAsync($"HUBUSERS-{hubKey}", async entry =>
	            {
		            entry.SlidingExpiration = TimeSpan.FromHours(1);

		            var hubUsers = await DbContext.DexihHubUser.Where(c => c.HubKey == hubKey && c.IsValid).ToListAsync();
		            var users = await DbContext.Users.Where(c => hubUsers.Select(d => d.UserId).Contains(c.Id)).ToListAsync();

		            var hubUsersList = new List<HubUser>();
		            foreach(var hubUser in hubUsers)
		            {
			            var user = users.SingleOrDefault(c => c.Id == hubUser.UserId);
			            if (user != null)
			            {
				            hubUsersList.Add(new HubUser()
				            {
					            Email = user.Email,
					            FirstName = user.FirstName,
					            LastName = user.LastName,
					            Id = hubUser.UserId,
					            Permission = hubUser.Permission
				            });
			            }
		            }

		            return hubUsersList;
	            });

	            return returnList;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Error getting hub users.  {ex.Message}", ex);
            }
        }

        public class HubUser
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
            public string Email { get; set; }
            public string Id { get; set; }
            public DexihHubUser.EPermission Permission { get; set; }
        }

        public async Task<DexihHub> GetUserHub(long hubKey, string userId, bool isAdmin)
		{
			if (isAdmin)
			{
				return await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.IsValid);
			}
			else
			{
				var hub = await DbContext.DexihHubUser.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == userId && c.IsValid);
				if (hub == null)
				{
					throw new RepositoryManagerException($"A hub with key {hubKey} is not available to the current user.");
				}
				return await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.IsValid);
			}
		}

		public async Task<string> GetHubEncryptionKey(long hubKey)
		{
			var hub = await DbContext.DexihHubs.SingleAsync(c => c.HubKey == hubKey);
			return hub.EncryptionKey;
		}

		public async Task<DexihHub> SaveHub(DexihHub hub)
		{
			try
			{
				var exists = await DbContext.DexihHubs.AnyAsync(c => c.Name == hub.Name && c.HubKey != hub.HubKey && c.IsValid);
				if (exists)
				{
                    throw new RepositoryManagerException($"A hub with the name {hub.Name} already exists.");
				}

				// no encryption key provide, then create a random one.
				if(string.IsNullOrEmpty(hub.EncryptionKey) && string.IsNullOrEmpty(hub.EncryptionKey))
				{
					hub.EncryptionKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey();
				}

				DexihHub dbHub;

				if (hub.HubKey > 0)
				{
					dbHub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hub.HubKey && c.IsValid);
					if (dbHub != null)
					{
						hub.CopyProperties(dbHub, true);
					}
					else
					{
                        throw new RepositoryManagerException($"The hub could not be saved as the hub contains the hub_key {hub.HubKey} that no longer exists in the repository.");
					}
				}
				else
				{
					dbHub = hub;
					DbContext.DexihHubs.Add(dbHub);
					dbHub.IsValid = true;
				}

                //save the hub to generate a hub key.
                await DbContext.SaveChangesAsync();
				ResetHubCache(hub.HubKey);

				return hub;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save hub {hub.Name} failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihHub[]> DeleteHubs(string userId, bool isAdmin, long[] hubKeys)
		{
			try
			{
				var dbHubs = await DbContext.DexihHubs
					.Where(c => hubKeys.Contains(c.HubKey))
					.ToArrayAsync();

				foreach (var dbHub in dbHubs)
				{
					if (!isAdmin)
					{
						var hubUser = await DbContext.DexihHubUser.SingleOrDefaultAsync(c => c.HubKey == dbHub.HubKey && c.UserId == userId && c.IsValid);
						if (hubUser == null || hubUser.Permission != DexihHubUser.EPermission.Owner)
						{
							throw new RepositoryManagerException($"Failed to delete the hub with name {dbHub.Name} as user does not have owner permission on this  hub.");
						}
					}

					dbHub.IsValid = false;
					ResetHubCache(dbHub.HubKey);
				}

                await DbContext.SaveChangesAsync();
				

                return dbHubs;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete hubs failed.  {ex.Message}", ex);
            }
        }


        public async Task HubSetUserPermissions(long hubKey, IEnumerable<string> userIds, DexihHubUser.EPermission permission)
        {
            try
            {
	            var usersHub = await DbContext.DexihHubUser.Where(c => c.HubKey == hubKey && userIds.Contains(c.UserId)).ToListAsync();

                foreach (var userId in userIds)
                {
	                var userHub = usersHub.SingleOrDefault(c => c.UserId == userId);
	                if (userHub == null)
	                {
		                userHub = new DexihHubUser
		                {
			                UserId = userId,
			                Permission = permission,
			                HubKey = hubKey,
			                IsValid = true
		                };

		                DbContext.DexihHubUser.Add(userHub);
	                }
	                else
	                {
		                userHub.Permission = permission;
		                userHub.IsValid = true;
	                }

                    await DbContext.SaveChangesAsync();
	                ResetHubCache(hubKey);
                }
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Hub add users failed.  {ex.Message}", ex);
            }
        }

    public async Task HubDeleteUsers(long hubKey, IEnumerable<string> userIds)
		{
            try
            {
	            var usersHub = await DbContext.DexihHubUser.Where(c => c.HubKey == hubKey && userIds.Contains(c.UserId)).ToListAsync();
	            foreach (var userHub in usersHub)
	            {
		            userHub.IsValid = false;
	            }
	            await DbContext.SaveChangesAsync();
	            ResetHubCache(hubKey);
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Hub delete users failed.  {ex.Message}", ex);
            }

        }
        #endregion

        #region Encrypt Functions
        public async Task<string> DecryptString(long hubKey, string value)
		{
			var key = await GetHubEncryptionKey(hubKey);
			var decryptResult = Dexih.Utils.Crypto.EncryptString.Decrypt(value, key, 1000);
			return decryptResult;
		}

		public async Task<string> EncryptString(long hubKey, string value)
		{
			var key = await GetHubEncryptionKey(hubKey);
			var encryptResult = Dexih.Utils.Crypto.EncryptString.Encrypt(value, key, 1000);
			return encryptResult;
		}

		#endregion

		#region Authorization Functions
		
		/// <summary>
		/// Returns the permission level that the user has access to the hub.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="hubKey"></param>
		/// <param name="isAdmin"></param>
		/// <returns></returns>
		/// <exception cref="ApplicationUserException"></exception>
		public Task<DexihHubUser.EPermission> ValidateHub(ApplicationUser user, long hubKey, bool isAdmin = false)
		{
			var validate = MemoryCache.GetOrCreateAsync($"PERMISSION-USER-{user.Id}-HUB-{hubKey}", async entry =>
			{
				entry.SlidingExpiration = TimeSpan.FromMinutes(1);

				if (!user.EmailConfirmed)
				{
					throw new ApplicationUserException("The users email address has not been confirmed.");
				}

				var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey);

				if (hub == null)
				{
					throw new ApplicationUserException("The hub with the key: " + hubKey + " could not be found.");
				}

				if (isAdmin)
				{
					return DexihHubUser.EPermission.Owner;
				}

				var hubUser =
					await DbContext.DexihHubUser.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == user.Id && c.IsValid);

				if (hubUser.Permission == DexihHubUser.EPermission.Suspended ||
				    hubUser.Permission == DexihHubUser.EPermission.None)
				{
					throw new ApplicationUserException($"The users does not have access to the hub with key {hubKey}.");
				}
				else
				{
					return hubUser.Permission;
				}
			});

			return validate;
		}
		
		#endregion
		
		
//		public string ApplyNamingStandard(string name, string param1)
//		{
//			if (_namingStandards.ContainsKey(name))
//			{
//				var setting = _namingStandards[name];
//				return setting.Value.Replace("{0}", param1);
//			}
//			else
//			{
//				throw new RepositoryManagerException($"The naming standard for the name \"{name}\" with parameter \"{param1}\" could not be found.");
//			}
//		}
//
//		/// <summary>
//		/// Cache the naming standards records.
//		/// </summary>
//		/// <returns></returns>
//		private async Task LoadNamingStandards()
//		{
//			try
//			{
//				_namingStandards = await DbContext.DexihSettings.Where(c => c.Category == "Naming").ToDictionaryAsync(n => n.Name);
//			}
//            catch (Exception ex)
//            {
//                throw new RepositoryManagerException($"Loading naming standards failed.  {ex.Message}", ex);
//            }
//        }

        #region Connection Functions
        public async Task<DexihConnection> SaveConnection(long hubKey, DexihConnection connection)
		{
			try
			{
				DexihConnection dbConnection;

				//check there are no connections with the same name
				var sameName = await DbContext.DexihConnections.FirstOrDefaultAsync(c => 
                        c.HubKey == hubKey &&
                        c.Name == connection.Name && 
                        c.ConnectionKey != connection.ConnectionKey && 
                        c.IsValid);

				if (sameName != null)
				{
                    throw new RepositoryManagerException($"The name \"{connection.Name}\" already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (connection.ConnectionKey > 0)
				{
					dbConnection = await DbContext.DexihConnections.SingleOrDefaultAsync(d => d.ConnectionKey == connection.ConnectionKey);
					if (dbConnection != null)
					{
						connection.CopyProperties(dbConnection);
					}
					else
					{
                        throw new RepositoryManagerException($"An update was attempted, however the connection_key {connection.ConnectionKey} no longer exists in the repository.");
                    }
                }
				else
                {
                    dbConnection = connection;
					DbContext.DexihConnections.Add(dbConnection);
				}

				if (!string.IsNullOrEmpty(dbConnection.PasswordRaw))
				{
					// if the UsePasswordVariable variable is set, then do not encrypt the password (which will be a variable name).
					dbConnection.Password = dbConnection.UsePasswordVariable ? dbConnection.PasswordRaw : await EncryptString(connection.HubKey, dbConnection.PasswordRaw);
				}

				if (!string.IsNullOrEmpty(dbConnection.ConnectionStringRaw))
				{
					// if the UseConnectionStringVariable is set, then do not encrypt the password (which will be a variable name).
					dbConnection.ConnectionString = dbConnection.UseConnectionStringVariable ? dbConnection.ConnectionStringRaw : await EncryptString(connection.HubKey, dbConnection.ConnectionStringRaw);
				}

				dbConnection.HubKey = hubKey;
				dbConnection.IsValid = true;

				await SaveHubChangesAsync(hubKey);
				
				// var dbConnection2 = await DbContext.DexihConnections.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.ConnectionKey == dbConnection.ConnectionKey);

				return dbConnection;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save connection \"{connection.Name}\" failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihConnection[]> DeleteConnections(long hubKey, long[] connectionKeys)
		{
            try
            {
                var dbConnections = await DbContext.DexihConnections
                    .Include(connection => connection.DexihTables)
                    .ThenInclude(table => table.DexihTableColumns)
                    .Where(c => c.HubKey == hubKey && connectionKeys.Contains(c.ConnectionKey) && c.IsValid)
                    .ToArrayAsync();

                foreach (var connection in dbConnections)
                {
                    connection.IsValid = false;
                    foreach (var table in connection.DexihTables)
                    {
                        table.IsValid = false;

                        foreach (var column in table.DexihTableColumns)
                        {
                            column.IsValid = false;
                        }
                    }
                }

                await SaveHubChangesAsync(hubKey);

                return dbConnections;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete connections failed.  {ex.Message}", ex);
            }
        }



        public async Task<DexihConnection> GetConnection(long hubKey, long connectionKey, bool includeTables)
		{
            try
            {
                var dbConnection = await DbContext.DexihConnections
                      .SingleOrDefaultAsync(c => c.ConnectionKey == connectionKey && c.HubKey == hubKey && c.IsValid);

                if (dbConnection == null)
                {
                    throw new RepositoryManagerException($"The connection with the key {connectionKey} could not be found.");
                }

                if (includeTables)
                {
                    var cache = new CacheManager(dbConnection.HubKey, "");
                    await cache.AddConnections(new[] { connectionKey }, true, DbContext);
                    dbConnection = cache.Hub.DexihConnections.First();
                }

                return dbConnection;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get connection with key {connectionKey} failed.  {ex.Message}", ex);
            }

        }

        public async Task<long> GetInternalConnectionKey(long hubKey)
		{
			if (_internalConnectionKey == 0)
			{
				var dbConnection = await DbContext.DexihConnections.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.IsInternal);
				if (dbConnection == null)
				{
					throw new RepositoryManagerException($"There is no internal connection in the hub with key {hubKey}.");
				}

				_internalConnectionKey = dbConnection.ConnectionKey;
			}

			return _internalConnectionKey;
		} /**/
		#endregion

		#region Table Functions
		public async Task<DexihTable[]> SaveTables(long hubKey, IEnumerable<DexihTable> tables, bool includeColumns, bool includeFileFormat = false)
		{
			try
			{
				var savedTables = new List<DexihTable>();
				foreach (var table in tables)
				{
					DexihTable dbTable;

					//check there are no datajobs with the same name
					var sameName = await DbContext.DexihTables.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.ConnectionKey == table.ConnectionKey && c.Name == table.Name && c.TableKey != table.TableKey && c.IsValid);
					if (sameName != null)
					{
                        throw new RepositoryManagerException($"A table with the name {table.Name} already exists in the repository.");
					}

					if (!includeColumns)
					{
						table.DexihTableColumns = null;
					}

					if (includeFileFormat && table.FileFormat != null)
					{
						var dbFileFormat = await DbContext.DexihFileFormat.SingleOrDefaultAsync(f => f.HubKey == hubKey && f.FileFormatKey == table.FileFormat.FileFormatKey);
						if (dbFileFormat == null)
						{
							table.EntityStatus.Message = $"The table could not be saved as the table contains the fileformat {table.FileFormat.FileFormatKey} that no longer exists in the repository.";
							table.EntityStatus.LastStatus = EntityStatus.EStatus.Error;
                            throw new RepositoryManagerException(table.EntityStatus.Message);
                        }

                        table.FileFormat.CopyProperties(dbFileFormat, true);
						table.FileFormat = dbFileFormat;
					}

					var dbConnection = DbContext.DexihConnections.SingleOrDefault(c => c.HubKey == hubKey && c.ConnectionKey == table.ConnectionKey);
                    if (dbConnection == null)
                    {
                        table.EntityStatus.Message = $"The table could not be saved as the table contains connection that no longer exists in the repository.";
                        table.EntityStatus.LastStatus = EntityStatus.EStatus.Error;
                        throw new RepositoryManagerException(table.EntityStatus.Message);
                    }

                    // use the table name where the base has not been set.
                    if (string.IsNullOrEmpty(table.BaseTableName) )
                    {
                        table.BaseTableName = table.Name;
                    }

					if (table.TableKey <= 0)
					{
						dbTable = new DexihTable();
						table.CopyProperties(dbTable, false);
						DbContext.DexihTables.Add(dbTable);
						savedTables.Add(dbTable);
					}
					else
					{
						dbTable = await GetTable(hubKey, table.TableKey, true);
						
						if (dbTable == null)
						{
							dbTable = new DexihTable();
							table.CopyProperties(dbTable, false);
							DbContext.DexihTables.Add(dbTable);
							savedTables.Add(dbTable);
						}
						else
						{
							if (dbTable.FileFormat?.FileFormatKey == table.FileFormat?.FileFormatKey)
							{
								table.FileFormat = dbTable.FileFormat;
							}

							table.CopyProperties(dbTable, false);
							dbTable.UpdateDate = DateTime.Now; // chnage update date to force table to become modified entity.
							savedTables.Add(dbTable);
						}
					}

					// if (dbTable.TableKey < 0) dbTable.TableKey = 0;

					dbTable.IsValid = true;
					dbTable.HubKey = hubKey;
				}

				// remove any change tracking on the file format, to aovid an attempted resave.
				var entities = DbContext.ChangeTracker.Entries().Where(x => (
						x.Entity is DexihFileFormat ||
                        x.Entity is DexihColumnValidation
					) && (x.State == EntityState.Added || x.State == EntityState.Modified));
				entities.Select(c => { c.State = EntityState.Unchanged; return c; }).ToList();

				await SaveHubChangesAsync(hubKey);
				return savedTables.ToArray();
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save tables failed.  {ex.Message}", ex);
            }
        }


        public async Task<DexihTable[]> DeleteTables(long hubKey, long[] tableKeys)
		{
            try
            {
                var dbTables = await DbContext.DexihTables
                    .Include(d => d.DexihTableColumns)
                    .Include(f => f.FileFormat)
                    .Where(c => c.HubKey == hubKey && tableKeys.Contains(c.TableKey) && c.IsValid)
                    .ToArrayAsync();

                foreach (var table in dbTables)
                {
                    table.IsValid = false;

                    foreach (var dbColumn in table.DexihTableColumns)
                    {
                        dbColumn.IsValid = false;
                    }

                    if (table.FileFormat != null && table.FileFormat.IsDefault == false)
                    {
                        table.FileFormat.IsValid = false;
                    }
                }

                await SaveHubChangesAsync(hubKey);

                return dbTables;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete tables failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihTable[]> ShareTables(long hubKey, long[] tableKeys, bool isShared)
        {
            try
            {
                var dbTables = await DbContext.DexihTables
                    .Include(d => d.DexihTableColumns)
                    .Include(f => f.FileFormat)
                    .Where(c => c.HubKey == hubKey && tableKeys.Contains(c.TableKey) && c.IsValid)
                    .ToArrayAsync();

                foreach (var table in dbTables)
                {
                    table.IsShared = isShared;
                }

                await SaveHubChangesAsync(hubKey);

                return dbTables;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Share tables failed.  {ex.Message}", ex);
            }

        }
		
		public async Task<DexihTable[]> GetTables(long hubKey, IEnumerable<long> tableKeys, bool includeColumns)
		{
			try
			{
				var dbTables = await DbContext.DexihTables.Where(c => tableKeys.Contains(c.TableKey) && c.HubKey == hubKey && c.IsValid).ToArrayAsync();

				if (includeColumns)
				{
					await DbContext.DexihTableColumns
		               .Where(c => c.IsValid && c.HubKey == hubKey && tableKeys.Contains(c.TableKey))
						.LoadAsync();
				}

				return dbTables;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get table with keys {string.Join(",", tableKeys)} failed.  {ex.Message}", ex);
			}
		}
		

        public async Task<DexihTable> GetTable(long hubKey, long tableKey, bool includeColumns)
		{
            try
            {
                var dbTable = await DbContext.DexihTables
	                .SingleOrDefaultAsync(c => c.TableKey == tableKey && c.HubKey == hubKey && c.IsValid);

                if (dbTable == null)
                {
                    throw new RepositoryManagerException($"The table with the key {tableKey} could not be found.");
                }

	            if (dbTable.FileFormatKey != null)
	            {
		            dbTable.FileFormat =
			            await DbContext.DexihFileFormat.SingleOrDefaultAsync(
				            c => c.FileFormatKey == dbTable.FileFormatKey && c.IsValid);
	            }

                if (includeColumns)
                {
                    await DbContext.Entry(dbTable).Collection(a => a.DexihTableColumns).Query()
					               .Where(c => c.HubKey == hubKey && c.IsValid && dbTable.TableKey == c.TableKey).LoadAsync();
                }

                return dbTable;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get table with key {tableKey} failed.  {ex.Message}", ex);
            }
        }
        #endregion

        #region Datalink Functions
		

        /// <summary>
        /// This looks at the table attributes and attempts the best strategy.
        /// *** FUTURE: FUNCTION SHOULD BE IMPROVED TO SUGGEST BASED ON PROFILE DATA****
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public TransformDelta.EUpdateStrategy GetBestUpdateStrategy(DexihTable table)
		{
            try
            {
                //TODO Improve get best strategy 

	            if (table == null)
		            return TransformDelta.EUpdateStrategy.Reload;
	            else if (table.DexihTableColumns.Count(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey) == 0)
	            {
		            // no natrual key.  Reload is the only choice
		            return TransformDelta.EUpdateStrategy.Reload;
	            }
	            else
	            {
		            if (table.IsVersioned)
			            return TransformDelta.EUpdateStrategy.AppendUpdateDeletePreserve;
		            else
			            return TransformDelta.EUpdateStrategy.AppendUpdateDelete;
	            }
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get best update strategy failed.  {ex.Message}", ex);
            }

        }


        public async Task<DexihDatalink[]> SaveDatalinks(long hubKey, DexihDatalink[] datalinks, bool includeTargetTable)
		{
			try
			{
				var savedDatalinks = new List<DexihDatalink>();
				foreach (var datalink in datalinks)
				{
					//check there are no datajobs with the same name
					var sameName = await DbContext.DexihDatalinks.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == datalink.Name && c.DatalinkKey != datalink.DatalinkKey && c.IsValid);
					if (sameName != null)
					{
                        throw new RepositoryManagerException($"A datalink with the name {datalink.Name} already exists in the repository.");
					}


                    if (datalink.DatalinkKey <= 0)
                    {
                        var newDatalink = datalink.CloneProperties<DexihDatalink>();

						if(includeTargetTable && datalink.TargetTable != null) 
						{
							newDatalink.TargetTable = datalink.TargetTable;
						}

                        newDatalink.ResetDatalinkColumns();
                        DbContext.Add(newDatalink);
                        savedDatalinks.Add(newDatalink);
                    }
                    else 
                    {
	                    var cacheManager = new CacheManager(hubKey, "");
                        var existingDatalink = await cacheManager.GetDatalink(datalink.DatalinkKey, DbContext);

                        // get columns from the repository instance, and merge the tracked instances into the new one.
                        var existingColumns = existingDatalink.GetAllDatalinkColumns();
						var newColumns = datalink.GetAllDatalinkColumns();

						// copy newColumns over existing column instances
						foreach(var newColumn in newColumns.Values)
						{
							if(existingColumns.ContainsKey(newColumn.DatalinkColumnKey))
							{
								newColumn.CopyProperties(existingColumns[newColumn.DatalinkColumnKey]);
							}
						}

						// Reset columns ensures only one instance of each column exists.  
						// without this the entity framework tries to insert record twice causing PK violations.
                        datalink.ResetDatalinkColumns(existingColumns);

                        datalink.CopyProperties(existingDatalink);
                        existingDatalink.ResetDatalinkColumns();
	                    existingDatalink.UpdateDate = DateTime.Now;
                        savedDatalinks.Add(existingDatalink);
                    } 
                }

                // uncomment to check changes.
                //var modifiedEntries = DbContext.ChangeTracker
                   //.Entries()
                   //.Where(x => x.State == EntityState.Modified || x.State == EntityState.Added || x.State == EntityState.Deleted)
                   //.Select(x => x)
                   //.ToList();

                await SaveHubChangesAsync(hubKey);
                //await DbContext.DexihUpdateStrategies.LoadAsync();
				 return savedDatalinks.ToArray();
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save datalinks failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihDatalink[]> DeleteDatalinks(long hubKey, long[] datalinkKeys)
		{
			try
			{
				var cache = new CacheManager(hubKey, "");
				var dbDatalinks = await cache.GetDatalinks(datalinkKeys, DbContext);

				foreach (var dbDatalink in dbDatalinks)
				{
					dbDatalink.IsValid = false;
					dbDatalink.DexihDatalinkTransforms.Select(c =>
					{
						c.IsValid = false;
						c.DexihDatalinkTransformItems.Select(i =>
						{
							i.IsValid = false;
							i.DexihFunctionParameters.Select(p =>
							{
								p.IsValid = false;
								p.ArrayParameters.Select(ap =>
								{
									ap.IsValid = false;
									return ap;
								}).ToList();
								return p;
							}).ToList();
							return i;
						}).ToList();
						return c;
					}).ToList();

					dbDatalink.DexihDatalinkProfiles.Select(p =>
					{
						p.IsValid = false;
						return p;
					}).ToList();

					dbDatalink.DexihDatalinkSteps.Select(s =>
					{
						s.IsValid = false;
						s.DexihDatalinkDependencies.Select(d =>
						{
							d.IsValid = false;
							return d;
						}).ToList();
						return s;
					}).ToList();
				}

				await SaveHubChangesAsync(hubKey);

				return dbDatalinks;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete datalinks failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihDatalink[]> ShareDatalinks(long hubKey, long[] datalinkKeys, bool isShared)
        {
            try
            {
	            var cache = new CacheManager(hubKey, "");
	            var dbDatalinks = await cache.GetDatalinks(datalinkKeys, DbContext); 
	                
                foreach (var dbDatalink in dbDatalinks)
                {
                    dbDatalink.IsShared = isShared;
                }

                await SaveHubChangesAsync(hubKey);

                return dbDatalinks;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Share datalinks failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihDatalink[]> NewDatalinks(long hubKey, 
	        string datalinkName, 
	        DexihDatalink.EDatalinkType datalinkType, 
	        long? targetConnectionKey, 
	        long[] sourceTableKeys, 
	        long? targetTableKey, 
	        string targetTableName, 
	        long? auditConnectionKey, 
	        bool addSourceColumns,
	        EDeltaType[] auditColumns,
	        NamingStandards namingStandards)
		{
			try
			{
				if (namingStandards == null)
				{
					namingStandards = new NamingStandards();
				}

				var newDatalinks = new List<DexihDatalink>();
				var newTargetTables = new List<DexihTable>();

				if (sourceTableKeys.Length == 0)
				{
					sourceTableKeys = new long[] { 0 };
				}

                long tempColumnKeys = -1;

				var sourceTables = await DbContext.DexihTables.Where(c => c.HubKey == hubKey && sourceTableKeys.Contains(c.TableKey) && c.IsValid).ToDictionaryAsync(c => c.TableKey);
				await DbContext.DexihTableColumns.Where(c=> c.HubKey == hubKey && sourceTables.Keys.Contains(c.TableKey) && c.IsValid).LoadAsync();
				var targetTable = targetTableKey == null ? null : await DbContext.DexihTables.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.TableKey == targetTableKey);
				var targetCon = targetConnectionKey == null ? null : await DbContext.DexihConnections.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.ConnectionKey == targetConnectionKey);

				foreach (var sourceTableKey in sourceTableKeys)
				{
					if (sourceTableKey == 0 && targetTableKey != null && targetTableName != null)
					{
                        throw new RepositoryManagerException("There is no source table selected, so the target table cannot be autonamed and must be defined.");
					}

					if (!string.IsNullOrEmpty(datalinkName) && await DbContext.DexihDatalinks.AnyAsync(c => c.Name == datalinkName && c.IsValid))
					{
                        throw new RepositoryManagerException("There is already an existing datalink with the name " + datalinkName + ".");
					}

					var sourceTable = sourceTables[sourceTableKey];

					if (sourceTable == null)
					{
						throw new RepositoryManagerException($"The source table with the key {sourceTableKey} does not exist in the repository.");
					}

					var newDatalinkName = datalinkName;

					// if the generated name exists, incrementally add a counter to the name until a free one is found.
					if (string.IsNullOrEmpty(newDatalinkName))
					{
						var count = 0;
						var baseName = namingStandards.ApplyNamingStandard(datalinkType + ".Datalink.Name", sourceTable.Name);
						string[] newName = {baseName};
						while (await DbContext.DexihDatalinks.AnyAsync(c => c.HubKey == hubKey && c.Name == newName[0] && c.IsValid))
						{
							newName[0] = $"{baseName} ({count++})";

							if (count > 100)
							{
                                throw new RepositoryManagerException("An unexpected nested loop occurred when attempting to create the datalink name.");
							}
						}
                        newDatalinkName = newName[0];
					}

					var updateStrategy = GetBestUpdateStrategy(sourceTable);

					var datalink = new DexihDatalink
					{
						HubKey = hubKey,
						Name = newDatalinkName,
						UpdateStrategy = updateStrategy,
						DatalinkType = datalinkType,
						AuditConnectionKey = auditConnectionKey,
						MaxRows = 1000,
						RowsPerProgress = 1000,
						IsValid = true,
						SourceDatalinkTable = new DexihDatalinkTable()
						{
							SourceType = DexihDatalinkTable.ESourceType.Table,
							SourceTableKey = sourceTable?.TableKey,
							SourceTable = sourceTable,
							Name = sourceTable?.Name
						}
					};

					// create a copy of the source table columns, and convert to dexihdatalinkColumn type.
					foreach (var column in sourceTable.DexihTableColumns.OrderBy(c => c.Position))
					{
						var newColumn = new DexihDatalinkColumn();
						column.CopyProperties(newColumn, true);
						newColumn.DatalinkColumnKey = tempColumnKeys--;
						datalink.SourceDatalinkTable.DexihDatalinkColumns.Add(newColumn);
					}

                    // if there is no target table specified, then create one with the default columns mapped from the source table.
                    if (targetTableKey == null)
                    {
	                    if (targetCon == null)
	                    {
		                    datalink.VirtualTargetTable = true;
	                    }
	                    else
	                    {
		                    targetTable = await CreateDefaultTargetTable(hubKey, datalinkType, sourceTable, targetTableName, targetCon, addSourceColumns, auditColumns, namingStandards);
		                    datalink.TargetTable = targetTable;
		                    datalink.VirtualTargetTable = false;
	                    }
                    }
                    else
                    {
                        datalink.TargetTableKey = targetTableKey;
	                    datalink.VirtualTargetTable = false;
					}

					//add a mapping transform, with source/target fields mapped.
					var mappingTransform = Transforms.GetDefaultMappingTransform();
					var datalinkTransform = CreateDefaultDatalinkTransform(hubKey, mappingTransform);
					datalinkTransform.PassThroughColumns = true;

					if (targetTable != null)
					{
						var position = 1;
						foreach (var targetColumn in targetTable.DexihTableColumns)
						{
							var sourceColumn =
								datalink.SourceDatalinkTable.DexihDatalinkColumns.FirstOrDefault(c =>
									c.LogicalName == targetColumn.LogicalName);

							//any columns which have been renamed in the target table, will get a mapping created for them, as passthrough won't map
							if (sourceColumn != null && sourceColumn.Name != targetColumn.Name)
							{
								var item = new DexihDatalinkTransformItem()
								{
									Position = position++,
									TransformItemType = DexihDatalinkTransformItem.ETransformItemType.ColumnPair
								};

								item.SourceDatalinkColumn = sourceColumn;

								var newColumn = new DexihDatalinkColumn();
								targetColumn.CopyProperties(newColumn, true);
								newColumn.DatalinkColumnKey = tempColumnKeys--;
								item.TargetDatalinkColumn = newColumn;
								item.TargetDatalinkColumnKey = 0;

								datalinkTransform.DexihDatalinkTransformItems.Add(item);
							}
						}
					}

					datalink.DexihDatalinkTransforms.Add(datalinkTransform);

                    datalink.ProfileTableName = namingStandards.ApplyNamingStandard("Table.ProfileName", targetTableName);

                    newDatalinks.Add(datalink);
				}

				var savedDatalinks = await SaveDatalinks(hubKey, newDatalinks.ToArray(), true);
				return savedDatalinks;

			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Create new datalinks failed.  {ex.Message}", ex);
            }

        }
        #endregion

        #region Datajob Functions

        public async Task<DexihDatajob[]> GetDatajobs(long hubKey, IEnumerable<long> datajobKeys)
		{
			try
			{
				var datajobs = await DbContext.DexihDatajobs
								   .Include(t => t.DexihTriggers)
								   .Include(s => s.DexihDatalinkSteps).ThenInclude(d => d.DexihDatalinkDependencies)
								   .Where(j => datajobKeys.Contains(j.DatajobKey) && j.IsValid && j.HubKey == hubKey).ToArrayAsync();

				return datajobs;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get datajob with keys {string.Join(",", datajobKeys)} failed.  {ex.Message}", ex);
            }

        }

		public async Task<DexihDatajob> GetDatajob(long hubKey, long datajobKey)
		{
			var datajobs = await GetDatajobs(hubKey, new long[] {datajobKey});
			if (datajobs.Length == 1)
			{
				return datajobs[0];
			}

			return null;
		}

        public async Task<DexihDatajob[]> SaveDatajobs(long hubKey, DexihDatajob[] datajobs)
		{
			try
			{
				var savedDatajobs = new List<DexihDatajob>();
				foreach (var datajob in datajobs)
				{
					if (string.IsNullOrEmpty(datajob.Name))
					{
						throw new RepositoryManagerException($"The datajob requires a name.");
					}
					
					//check there are no datajobs with the same name
					var sameName = await DbContext.DexihDatajobs.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == datajob.Name && c.DatajobKey != datajob.DatajobKey && c.IsValid);
					if (sameName != null)
					{
                        throw new RepositoryManagerException($"A datajob with the name {datajob.Name} already exists in the repository.");
					}

                    foreach(var step in datajob.DexihDatalinkSteps)
                    {
                        foreach(var dep in step.DexihDatalinkDependencies)
                        {
                            if (dep.DatalinkDependencyKey < 0)
                            {
                                dep.DatalinkDependencyKey = 0;
                            }

                            if (dep.DependentDatalinkStepKey <= 0)
                            {
                                dep.DependentDatalinkStep = datajob.DexihDatalinkSteps.SingleOrDefault(c => c.DatalinkStepKey == dep.DependentDatalinkStepKey);
                                dep.DatalinkStepKey = 0;
                            }
                        }
                    }

                    foreach (var step in datajob.DexihDatalinkSteps)
                    {
                        if (step.DatalinkStepKey < 0)
                        {
                            step.DatalinkStepKey = 0;
                        }
                    }

                    if (datajob.DatajobKey <= 0) {
						datajob.DatajobKey = 0;
						// var newDatajob = new DexihDatajob();
						// datajob.CopyProperties(newDatajob, false);
						DbContext.DexihDatajobs.Add(datajob);
						savedDatajobs.Add(datajob);
					}
					else
					{
						var originalDatajob = await GetDatajob(hubKey, datajob.DatajobKey);

						if(originalDatajob == null)
						{
							datajob.DatajobKey = 0;
							var newDatajob = new DexihDatajob();
							datajob.CopyProperties(newDatajob, false);
							DbContext.DexihDatajobs.Add(newDatajob);
							savedDatajobs.Add(datajob);
						}
						else 
						{
							datajob.CopyProperties(originalDatajob);
							originalDatajob.UpdateDate = DateTime.Now;
							savedDatajobs.Add(originalDatajob);
						}
					}
				}

				await SaveHubChangesAsync(hubKey);
				return savedDatajobs.ToArray();
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save datajobs failed.  {ex.Message}", ex);
            }
        }

        public async Task<bool> DeleteDatajobs(long hubKey, long[] datajobKeys)
		{
			try
			{
				var datajobs = await DbContext.DexihDatajobs
					.Include(d => d.DexihTriggers)
					.Include(s => s.DexihDatalinkSteps)
						.ThenInclude(d => d.DexihDatalinkDependencies)
					.Where(c => c.HubKey == hubKey && datajobKeys.Contains(c.DatajobKey))
					.ToArrayAsync();

				foreach (var datajob in datajobs)
				{
					datajob.IsValid = false;

					foreach (var trigger in datajob.DexihTriggers)
					{
						trigger.IsValid = false;
					}

					foreach (var datastep in datajob.DexihDatalinkSteps)
					{
						datastep.IsValid = false;

						foreach (var dep in datastep.DexihDatalinkDependencies)
						{
							dep.IsValid = false;
						}
					}
				}

				await SaveHubChangesAsync(hubKey);
				return true;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete datajobs failed.  {ex.Message}", ex);
            }
        }

        #endregion

        #region RemoteAgent Functions

		/// <summary>
		/// Checks if a remote agent is authorized to access the hub based on the remoteagent id and originating ip address.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="iPAddress"></param>
		/// <param name="remoteSettings"></param>
		/// <returns></returns>
		public async Task<DexihRemoteAgent> RemoteAgentLogin(string iPAddress, string remoteAgentId)
		{
            try
            {
	            var remoteAgent = await DbContext.DexihRemoteAgents.SingleOrDefaultAsync(c => 
		            c.RemoteAgentId == remoteAgentId && 
		            c.IsValid);

	            if (remoteAgent == null)
	            {
		            return null;
	            }

	            remoteAgent.LastLoginDateTime = DateTime.Now;
				remoteAgent.LastLoginIpAddress = iPAddress;
				await DbContext.SaveChangesAsync();
				return remoteAgent;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Remote agent login failed.  {ex.Message}", ex);
            }
        }
		
		public Task<DexihRemoteAgent> GetRemoteAgent(long remoteAgentKey)
		{
			return DbContext.DexihRemoteAgents.SingleOrDefaultAsync(
				c => c.RemoteAgentKey == remoteAgentKey && c.IsValid);
		}
		
	   public async Task<DexihRemoteAgent> SaveRemoteAgent(string userId, DexihRemoteAgent remoteAgent)
		{
            try
            {
	            DexihRemoteAgent dbRemoteAgent;
	            
                //if there is a remoteAgentKey, retrieve the record from the database, and copy the properties across.
                if (remoteAgent.RemoteAgentKey > 0)
                {
                    dbRemoteAgent = await DbContext.DexihRemoteAgents.SingleOrDefaultAsync(d => d.RemoteAgentKey == remoteAgent.RemoteAgentKey && d.IsValid);
                    if (dbRemoteAgent != null)
                    {
	                    if (dbRemoteAgent.UserId == userId)
	                    {
		                    remoteAgent.CopyProperties(dbRemoteAgent, true);
		                    dbRemoteAgent.UserId = userId;
	                    }
	                    else
	                    {
		                    throw new RepositoryManagerException("The remote agent could not be updated as the current user is different from the logged in user.  To save with a different user, delete the existing instance, and then save again.");
	                    }
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The remote agent could not be saved as the remote agent contains the remoteagent_key {remoteAgent.RemoteAgentId} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbRemoteAgent = remoteAgent;
	                if (dbRemoteAgent.UserId == userId)
	                {
		                DbContext.DexihRemoteAgents.Add(dbRemoteAgent);
	                }
	                else
	                {
		                throw new RepositoryManagerException("The remote agent could not be updated as the current user is different from the user that logged into the remote agent.");
	                }
                }

                dbRemoteAgent.IsValid = true;

	            await DbContext.SaveChangesAsync();

                return dbRemoteAgent;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save remote agents failed.  {ex.Message}", ex);
            }
        }
		
		public async Task<bool> DeleteRemoteAgent(long remoteAgentKey)
		{
			try
			{
				var dbItem = await DbContext.DexihRemoteAgents
					.SingleAsync(c => c.RemoteAgentKey == remoteAgentKey);

				dbItem.IsValid = false;
				await DbContext.SaveChangesAsync();

				return true;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Delete remote agents failed.  {ex.Message}", ex);
			}
		}

		/// <summary>
		/// Gets a list of all hubs which have been authorized for the remote agent.
		/// </summary>
		/// <param name="iPAddress"></param>
		/// <param name="remoteSettings"></param>
		/// <returns></returns>
		public async Task<DexihRemoteAgentHub[]> AuthorizedRemoteAgentHubs(string iPAddress, RemoteSettings remoteSettings)
		{
			return await MemoryCache.GetOrCreateAsync($"REMOTEAGENT-HUBS-{remoteSettings.AppSettings.RemoteAgentId}",
				async entry =>
				{
					entry.SlidingExpiration = TimeSpan.FromMinutes(1); 

					var hubs = await GetUserHubs(remoteSettings.Runtime.UserId, remoteSettings.Runtime.IsAdmin);
			
					var remoteAgents = await DbContext.DexihRemoteAgentHubs.Where(c => 
						c.RemoteAgent.RemoteAgentId == remoteSettings.AppSettings.RemoteAgentId &&
						hubs.Select(h => h.HubKey).Contains(c.HubKey) &&
						c.IsAuthorized &&
						c.IsValid).ToArrayAsync();

					return remoteAgents;
				});
		}
		
		/// <summary>
		/// Gets a list of all remote agents available to the user.
		/// </summary>
		/// <param name="userId"></param>
		/// <param name="isAdmin"></param>
		/// <returns></returns>
		public async Task<DexihRemoteAgentHub[]> AuthorizedUserRemoteAgentHubs(string userId, bool isAdmin)
		{
			return await MemoryCache.GetOrCreateAsync($"REMOTEAGENT-USER-HUBS-{userId}",
				async entry =>
				{
					entry.SlidingExpiration = TimeSpan.FromMinutes(1); 

					var hubs = await GetUserHubs(userId, isAdmin);
			
					var remoteAgents = await DbContext.DexihRemoteAgentHubs.Include(c => c.RemoteAgent).Where(c => 
						hubs.Select(h => h.HubKey).Contains(c.HubKey) &&
						c.IsAuthorized &&
						c.IsValid).ToArrayAsync();

					return remoteAgents;
				});
		}

		public Task<DexihRemoteAgentHub> AuthorizedRemoteAgentHub(long hubKey, long remoteAgentKey)
		{
			return DbContext.DexihRemoteAgentHubs.FirstOrDefaultAsync(d => d.HubKey == hubKey && d.RemoteAgentKey == remoteAgentKey && d.IsAuthorized && d.IsValid);
		}

		public async Task<DexihRemoteAgent[]> GetRemoteAgents(string userId, bool isAdmin)
		{
			var userHubs = (await GetUserHubs(userId, isAdmin)).Select(c=>c.HubKey);
			
			var remoteAgents = await DbContext.DexihRemoteAgents.Where(c => 
				c.IsValid && 
				((isAdmin || c.UserId == userId) || c.DexihremoteAgentHubs.Any(d => userHubs.Contains(d.HubKey)))
				).ToArrayAsync();
			return remoteAgents;
		}

        public async Task<DexihRemoteAgentHub> SaveRemoteAgentHub(string userId, long hubKey, DexihRemoteAgentHub remoteAgent)
		{
            try
            {
	            DexihRemoteAgentHub dbRemoteAgent;
	            
                //if there is a remoteAgentKey, retrieve the record from the database, and copy the properties across.
                if (remoteAgent.RemoteAgentHubKey > 0)
                {
                    dbRemoteAgent = await DbContext.DexihRemoteAgentHubs.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.RemoteAgentHubKey == remoteAgent.RemoteAgentHubKey && d.IsValid);
                    if (dbRemoteAgent != null)
                    {
	                    remoteAgent.CopyProperties(dbRemoteAgent, true);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The remote agent could not be saved as the remote agent contains the remoteagent_key {remoteAgent.RemoteAgentHubKey} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbRemoteAgent = remoteAgent;
					DbContext.DexihRemoteAgentHubs.Add(dbRemoteAgent);
                }

                dbRemoteAgent.IsValid = true;

                await SaveHubChangesAsync(hubKey);

                return dbRemoteAgent;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save remote agents failed.  {ex.Message}", ex);
            }

        }

        public async Task<bool> DeleteRemoteAgentHub(long hubKey, long remoteAgentHubKey)
		{
            try
            {
                var dbItem = await DbContext.DexihRemoteAgentHubs
                    .SingleAsync(c => c.HubKey == hubKey && c.RemoteAgentHubKey == remoteAgentHubKey);

                dbItem.IsValid = false;
                await SaveHubChangesAsync(hubKey);

                return true;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete remote agents failed.  {ex.Message}", ex);
            }
        }
		
        #endregion

        #region Datalink Transform Functions

        private DexihDatalinkTransform CreateDefaultDatalinkTransform(long hubKey, TransformReference transform)
		{
            try
            {
                var datalinkTransform = new DexihDatalinkTransform()
                {
	                HubKey = hubKey,
	                TransformAssemblyName = transform.TransformAssemblyName,
                    TransformClassName = transform.TransformClassName,
                    Name = transform.Name,
                    Description = transform.Description,
                    IsValid = true
                };

                return datalinkTransform;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Create default datalink transform failed.  {ex.Message}", ex);
            }

        }
        #endregion

        private async Task<DexihTable> CreateDefaultTargetTable(
	        long hubKey, 
	        DexihDatalink.EDatalinkType datalinkType, 
	        DexihTable sourceTable, 
	        string tableName, 
	        DexihConnection targetConnection,
	        bool addSourceColumns,
	        EDeltaType[] auditColumns,
	        NamingStandards namingStandards)
		{
            try
            {
                DexihTable table = null;

                if (namingStandards == null)
                {
                    namingStandards = new NamingStandards();
                }
            
	            var position = 1;

                if (sourceTable == null)
                {
                    if (string.IsNullOrEmpty(tableName))
                    {
                        throw new RepositoryManagerException("There is no name specified for the validation table, and no source table to auto-name the table from.");
                    }

					if(await DbContext.DexihTables.AnyAsync(c => c.HubKey == hubKey && c.ConnectionKey == targetConnection.ConnectionKey && c.Name == tableName && c.IsValid))
					{
						throw new RepositoryManagerException($"The target table could not be created as a table with the name {tableName} already exists.");
					}

                    table = new DexihTable()
                    {
                        HubKey = hubKey,
                        ConnectionKey = targetConnection.ConnectionKey,
						Name = tableName,
                        BaseTableName = tableName,
                        LogicalName = tableName,
                        Description = namingStandards.ApplyNamingStandard(datalinkType + ".Table.Description", tableName),
                        RejectedTableName = "",
                    };
                }
                else
                {
					var count = 0;
					var baseName = namingStandards.ApplyNamingStandard(datalinkType + ".Table.Name", sourceTable.BaseTableName);
					var newName = baseName;
					while (await DbContext.DexihTables.AnyAsync(c => c.HubKey == hubKey && c.ConnectionKey == targetConnection.ConnectionKey && c.Name == newName && c.IsValid))
					{
						newName = $"{baseName}_{count++}";

						if (count > 100)
						{
							throw new RepositoryManagerException("An unexpected nested loop occurred when attempting to create the datalink name.");
						}
					}

                    table = new DexihTable()
                    {
                        HubKey = hubKey,
                        ConnectionKey = targetConnection.ConnectionKey,
                        Name = newName,
                        BaseTableName = sourceTable.Name,
                        LogicalName = sourceTable.LogicalName,
                        Description = namingStandards.ApplyNamingStandard(datalinkType + ".Table.Description", sourceTable.BaseTableName),
                        RejectedTableName = namingStandards.ApplyNamingStandard("Table.RejectName", sourceTable.BaseTableName),
                    };
	                
	                //columns in the source table are added to the target table
	                if (addSourceColumns)
	                {
		                foreach (var col in sourceTable.DexihTableColumns.Where(c => c.IsSourceColumn).OrderBy(p => p.Position).ToList())
		                {
			                var newColumn = new DexihTableColumn();
			                col.CopyProperties(newColumn, true);
			                newColumn.TableKey = 0; // reset as this will be pointing to source table key.
			                newColumn.ColumnKey = 0;
			                newColumn.MapToTargetColumnProperties();
			                newColumn.Name = newColumn.Name.Replace(" ", " "); //TODO Add better removeUnsupportedCharacters to create target table
			                newColumn.Position = position++;
			                newColumn.IsValid = true;
			                table.DexihTableColumns.Add(newColumn);
		                }
	                }
	                
	                // if there is a surrogate key in the source table, then map it to a column to maintain lineage.
	                if (sourceTable.DexihTableColumns.Count(c => c.DeltaType == TableColumn.EDeltaType.SurrogateKey) > 0)
	                {
		                table.DexihTableColumns.Add(NewDefaultTableColumn("SourceSurrogateKey", namingStandards, table.Name, ETypeCode.Int64, EDeltaType.SourceSurrogateKey, position++));
	                }
                }

	            // add the default for each of the requested auditColumns.
	            foreach (var auditColumn in auditColumns)
	            {
		            var exists = table.DexihTableColumns.FirstOrDefault(c => c.DeltaType == auditColumn && c.IsValid);
		            if (exists == null)
		            {
			            var dataType = TableColumn.GetDeltaDataType(auditColumn);
			            var newColumn =
				            NewDefaultTableColumn(auditColumn.ToString(), namingStandards, table.Name, dataType, auditColumn, position++);

			            // ensure the name is unique.
			            string[] baseName = {newColumn.Name};
			            var version = 1;
			            while (table.DexihTableColumns.FirstOrDefault(c => c.Name == baseName[0] && c.IsValid) != null)
			            {
				            baseName[0] = newColumn.Name + (version++).ToString();
			            }
			            newColumn.Name = baseName[0];
				            
			            table.DexihTableColumns.Add(newColumn);
		            }
	            }

                table.IsValid = true;

                return table;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Create default target table failed.  {ex.Message}", ex);
            }
        }


        private DexihTableColumn NewDefaultTableColumn(string namingStandard, NamingStandards namingStandards, string tableName, ETypeCode dataType, EDeltaType deltaType, int position)
		{
            try
            {
                var col = new DexihTableColumn()
                {
                    Name = namingStandards.ApplyNamingStandard(namingStandard + ".Column.Name", tableName),
                    DataType = dataType,
                    AllowDbNull = false,
                    LogicalName = namingStandards.ApplyNamingStandard(namingStandard + ".Column.Logical", tableName),
                    Description = namingStandards.ApplyNamingStandard(namingStandard + ".Column.Description", tableName),
                    IsUnique = false,
                    DeltaType = deltaType,
                    Position = position,
                    IsValid = true
                };

                return col;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"New default table column failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihColumnValidation[]> DeleteColumnValidations(long hubKey, long[] columnValidationKeys)
		{
            try
            {
                var dbValidations = await DbContext.DexihColumnValidation
                    .Include(column => column.DexihColumnValidationColumn)
                    .Where(c => c.HubKey == hubKey && columnValidationKeys.Contains(c.ColumnValidationKey) && c.IsValid)
                    .ToArrayAsync();

                foreach (var validation in dbValidations)
                {
                    validation.IsValid = false;
                    foreach (var column in validation.DexihColumnValidationColumn)
                    {
                        column.ColumnValidationKey = null;
                    }
                }

                await SaveHubChangesAsync(hubKey);

                return dbValidations;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete column validations failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihColumnValidation> SaveColumnValidation(long hubKey, DexihColumnValidation validation)
		{
			try
			{
				DexihColumnValidation dbColumnValidation;

				//check there are no connections with the same name
				var sameName = await DbContext.DexihColumnValidation.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == validation.Name && c.ColumnValidationKey != validation.ColumnValidationKey && c.IsValid);
				if (sameName != null)
				{
                    throw new RepositoryManagerException($"A column validation with the name {validation.Name} already exists in the repository.");
				}

				//if there is a connectionKey, retrieve the record from the database, and copy the properties across.
				if (validation.ColumnValidationKey > 0)
				{
					dbColumnValidation = await DbContext.DexihColumnValidation.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.ColumnValidationKey == validation.ColumnValidationKey);
					if (dbColumnValidation != null)
					{
						validation.CopyProperties(dbColumnValidation, true);
					}
					else
					{
                        throw new RepositoryManagerException($"The column validation could not be saved as the validation contains the column_validation_key {validation.ColumnValidationKey} that no longer exists in the repository.");
					}
				}
				else
				{
					dbColumnValidation = new DexihColumnValidation();
					validation.CopyProperties(dbColumnValidation, true);
					DbContext.DexihColumnValidation.Add(dbColumnValidation);
				}


				dbColumnValidation.HubKey = hubKey;
				dbColumnValidation.IsValid = true;

				await SaveHubChangesAsync(hubKey);

				return dbColumnValidation;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save column validation {validation.Name} failed.  {ex.Message}", ex);
            }
        }
		
	  	public async Task<DexihCustomFunction[]> DeleteCustomFunctions(long hubKey, long[] customFunctionKeys)
		{
            try
            {
                var dbFunctions = await DbContext.DexihCustomFunctions
                    .Where(c => c.HubKey == hubKey && customFunctionKeys.Contains(c.CustomFunctionKey) && c.IsValid)
                    .ToArrayAsync();

                foreach (var function in dbFunctions)
                {
	                function.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey);

                return dbFunctions;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete custom functions failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihCustomFunction> SaveCustomFunction(long hubKey, DexihCustomFunction function)
		{
			try
			{
				DexihCustomFunction dbFunction;

				//check there are no connections with the same name
				var sameName = await DbContext.DexihCustomFunctions.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == function.Name && c.CustomFunctionKey != function.CustomFunctionKey && c.IsValid);
				if (sameName != null)
				{
                    throw new RepositoryManagerException($"A custom funciton with the name {function.Name} already exists in the hub.");
				}

				//if there is a connectionKey, retrieve the record from the database, and copy the properties across.
				if (function.CustomFunctionKey > 0)
				{
					dbFunction = await DbContext.DexihCustomFunctions.Include(c=>c.DexihCustomFunctionParameters).SingleOrDefaultAsync(d => d.HubKey == hubKey && d.CustomFunctionKey == function.CustomFunctionKey);
					
					if (dbFunction != null)
					{
						function.CopyProperties(dbFunction, false);
					}
					else
					{
                        throw new RepositoryManagerException($"The custom function could not be saved as the function contains the custom_function_key {function.CustomFunctionKey} that no longer exists in the hub.");
					}
				}
				else
				{
					dbFunction = function.CloneProperties<DexihCustomFunction>(false);
					DbContext.DexihCustomFunctions.Add(dbFunction);
				}


				dbFunction.HubKey = hubKey;
				dbFunction.IsValid = true;

			 var modifiedEntries = DbContext.ChangeTracker
				.Entries()
				.Where(x => x.State == EntityState.Modified || x.State == EntityState.Added || x.State == EntityState.Deleted)
				.Select(x => x)
				.ToList();
				
				await SaveHubChangesAsync(hubKey);

				return dbFunction;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save custom function {function.Name} failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihFileFormat[]> DeleteFileFormats(long hubKey, long[] fileFormatKeys)
		{
            try
            {
                var dbFileFormats = await DbContext.DexihFileFormat
                    .Where(c => c.HubKey == hubKey && fileFormatKeys.Contains(c.FileFormatKey) && c.IsValid)
                    .ToArrayAsync();

                foreach (var fileformat in dbFileFormats)
                {
                    fileformat.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey);

                return dbFileFormats;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete file formats failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihFileFormat> SaveFileFormat(long hubKey, DexihFileFormat fileformat)
		{
			try
			{
				DexihFileFormat dbFileFormat;

				//check there are no connections with the same name
				var sameName = await DbContext.DexihFileFormat.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == fileformat.Name && c.FileFormatKey != fileformat.FileFormatKey && c.IsValid);
				if (sameName != null)
				{
                    throw new RepositoryManagerException($"A file format with the name {fileformat.Name} already exists in the repository.");
				}

				//if there is a connectionKey, retrieve the record from the database, and copy the properties across.
				if (fileformat.FileFormatKey > 0)
				{
					dbFileFormat = await DbContext.DexihFileFormat.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.FileFormatKey == fileformat.FileFormatKey);
					if (dbFileFormat != null)
					{
						fileformat.CopyProperties(dbFileFormat, true);
					}
					else
					{
                        throw new RepositoryManagerException($"The file format could not be saved as it contains the file_format_key {fileformat.FileFormatKey} that no longer exists in the repository.");
					}
				}
				else
				{
					dbFileFormat = new DexihFileFormat();
					fileformat.CopyProperties(dbFileFormat, true);
					DbContext.DexihFileFormat.Add(dbFileFormat);
				}


				dbFileFormat.IsValid = true;

				await SaveHubChangesAsync(hubKey);

				return dbFileFormat;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save file format {fileformat.Name} failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihHubVariable[]> DeleteHubVariables(long hubKey, long[] hubVariableKeys)
        {
            try
            {
                var dbHubVariables = await DbContext.DexihHubVariable
                    .Where(c => c.HubKey == hubKey && hubVariableKeys.Contains(c.HubVariableKey) && c.IsValid)
                    .ToArrayAsync();

                foreach (var hubVariable in dbHubVariables)
                {
                    hubVariable.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey);

                return dbHubVariables;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete hub variables failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihHubVariable> SaveHubVariable(long hubKey, DexihHubVariable hubVariable)
        {
            try
            {
                DexihHubVariable dbHubVariable;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihHubVariable.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == hubVariable.Name && c.HubVariableKey != hubVariable.HubVariableKey && c.IsValid);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A variable with the name {hubVariable.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (hubVariable.HubVariableKey > 0)
                {
                    dbHubVariable = await DbContext.DexihHubVariable.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.HubVariableKey == hubVariable.HubVariableKey);
                    if (dbHubVariable != null)
                    {
                        hubVariable.CopyProperties(dbHubVariable, true);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The variable could not be saved as it contains the hub_variable_key {hubVariable.HubVariableKey} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbHubVariable = new DexihHubVariable();
                    hubVariable.CopyProperties(dbHubVariable, true);
                    DbContext.DexihHubVariable.Add(dbHubVariable);
                }


                dbHubVariable.IsValid = true;

                await SaveHubChangesAsync(hubKey);

                return dbHubVariable;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save file format {hubVariable.Name} failed.  {ex.Message}", ex);
            }
        }

		/// <summary>
		/// Compares an imported hub structure against the database structure, and maps keys and dependent obects together.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="hub">Importing Hub</param>
		/// <param name="connectionsAction"></param>
		/// <param name="tablesAction"></param>
		/// <param name="datalinksAction"></param>
		/// <param name="datajobsAction"></param>
		/// <param name="fileFormatsAction"></param>
		/// <param name="columnValidationsAction"></param>
		/// <returns></returns>
		/// <exception cref="ArgumentOutOfRangeException"></exception>
		public async Task<Import> CreateImportPlan(long hubKey, DexihHub hub, EImportAction hubVariableAction,  EImportAction connectionsAction,
			EImportAction tablesAction, EImportAction datalinksAction, EImportAction datajobsAction,
			EImportAction fileFormatsAction, EImportAction columnValidationsAction)
		{
			var keySequence = -1;
			
			var connectionKeyMappings = new Dictionary<long, long>();
			var tableKeyMappings = new Dictionary<long, long>();
			var columnKeyMappings = new Dictionary<long, long>();
			var datalinkKeyMappings = new Dictionary<long, long>();
			var datajobKeyMappings = new Dictionary<long, long>();
			var fileFormatKeyMappings = new Dictionary<long, long>();
			var columnValidationKeyMappings = new Dictionary<long, long>();
			var hubVariableKeyMappings = new Dictionary<long, long>();
			
			var plan = new Import(hubKey);

			var existingHubVariables =
				await DbContext.DexihHubVariable.Where(var => var.HubKey == hubKey && hub.DexihHubVariables.Select(c => c.Name).Contains(var.Name) && var.IsValid).ToArrayAsync();

			foreach (var hubVariable in hub.DexihHubVariables)
			{
				hubVariable.HubKey = hubKey;
				var existingHubVariable = existingHubVariables.SingleOrDefault(var => hubVariable.Name == var.Name);

				if (existingHubVariable != null)
				{
					switch (hubVariableAction)
					{
						case EImportAction.Replace:
							hubVariableKeyMappings.Add(hubVariable.HubVariableKey, existingHubVariable.HubVariableKey);
							hubVariable.HubVariableKey = existingHubVariable.HubVariableKey;
							plan.HubVariables.Add(hubVariable, EImportAction.Replace);
							break;
						case EImportAction.New:
							var newKey = keySequence--;
							hubVariableKeyMappings.Add(hubVariable.HubVariableKey, newKey);
							hubVariable.HubVariableKey = newKey;
							hubVariable.Name = hubVariable.Name + " - duplicate rename " + DateTime.Now;
							plan.HubVariables.Add(hubVariable, EImportAction.New);
							break;
						case EImportAction.Leave:
						case EImportAction.Skip:
							break;
						default:
							throw new ArgumentOutOfRangeException(nameof(columnValidationsAction), columnValidationsAction, null);
					}
				}
				else
				{
					if (hubVariableAction != EImportAction.Skip)
					{
						var newKey = keySequence--;
						hubVariableKeyMappings.Add(hubVariable.HubVariableKey, newKey);
						hubVariable.HubVariableKey = newKey;
						plan.HubVariables.Add(hubVariable, EImportAction.New);
					}
				}
			}

			var existingFileFormats =
				await DbContext.DexihFileFormat.Where(con => con.HubKey == hubKey && hub.DexihFileFormats.Select(c => c.Name).Contains(con.Name) && con.IsValid).ToArrayAsync();

			foreach (var fileFormat in hub.DexihFileFormats)
			{
				fileFormat.HubKey = hubKey;
				var existingFileFormat = existingFileFormats.SingleOrDefault(con => fileFormat.Name == con.Name);

				if (existingFileFormat != null)
				{
					switch (fileFormatsAction)
					{
						case EImportAction.Replace:
							fileFormatKeyMappings.Add(fileFormat.FileFormatKey, existingFileFormat.FileFormatKey);
							fileFormat.FileFormatKey = existingFileFormat.FileFormatKey;
							plan.FileFormats.Add(fileFormat, EImportAction.Replace);
							break;
						case EImportAction.New:
							var newKey = keySequence--;
							fileFormatKeyMappings.Add(fileFormat.FileFormatKey, newKey);
							fileFormat.FileFormatKey = newKey;
							fileFormat.Name = fileFormat.Name + " - duplicate rename " + DateTime.Now;
							plan.FileFormats.Add(fileFormat, EImportAction.New);
							break;
						case EImportAction.Leave:
						case EImportAction.Skip:
							break;
						default:
							throw new ArgumentOutOfRangeException(nameof(columnValidationsAction), columnValidationsAction, null);
					}
				}
				else
				{
					if (fileFormatsAction != EImportAction.Skip)
					{
						var newKey = keySequence--;
						fileFormatKeyMappings.Add(fileFormat.FileFormatKey, newKey);
						fileFormat.FileFormatKey = newKey;
						plan.FileFormats.Add(fileFormat, EImportAction.New);
					}
				}
			}

			var existingColumnValidations =
				await DbContext.DexihColumnValidation.Where(cv => cv.HubKey == hubKey && hub.DexihColumnValidations.Select(c => c.Name).Contains(cv.Name) && cv.IsValid).ToArrayAsync();


			foreach (var columnValidation in hub.DexihColumnValidations)
			{
				columnValidation.HubKey = hubKey;
				
				var existingcolumnValidation = existingColumnValidations.SingleOrDefault(con => columnValidation.Name == con.Name);

				if (existingcolumnValidation != null)
				{
					switch (columnValidationsAction)
					{
						case EImportAction.Replace:
							columnValidationKeyMappings.Add(columnValidation.ColumnValidationKey, existingcolumnValidation.ColumnValidationKey);
							columnValidation.ColumnValidationKey = existingcolumnValidation.ColumnValidationKey;
							plan.ColumnValidations.Add(columnValidation, EImportAction.Replace);
							break;
						case EImportAction.New:
							var newKey = keySequence--;
							columnValidationKeyMappings.Add(columnValidation.ColumnValidationKey, newKey);
							columnValidation.ColumnValidationKey = newKey;
							columnValidation.Name = columnValidation.Name + " - duplicate rename " + DateTime.Now;
							plan.ColumnValidations.Add(columnValidation, EImportAction.New);
							break;
						case EImportAction.Leave:
						case EImportAction.Skip:
							break;
						default:
							throw new ArgumentOutOfRangeException(nameof(columnValidationsAction), columnValidationsAction, null);
					}
				}
				else
				{
					if (columnValidationsAction != EImportAction.Skip)
					{
						var newKey = keySequence--;
						columnValidationKeyMappings.Add(columnValidation.ColumnValidationKey, newKey);
						columnValidation.ColumnValidationKey = newKey;
						plan.ColumnValidations.Add(columnValidation, EImportAction.New);
					}
				}
			}
			
			var existingConnections =
				await DbContext.DexihConnections.Where(con => con.HubKey == hubKey && hub.DexihConnections.Select(c => c.Name).Contains(con.Name) && con.IsValid).ToArrayAsync();

			
			foreach (var connection in hub.DexihConnections)
			{
				connection.HubKey = hubKey;
				var tables = connection.DexihTables;
				connection.DexihTables = null;
				
				var existingConnection = existingConnections.SingleOrDefault(con => connection.Name == con.Name);
				var isNewConnection = false;

				void NewConnection()
				{
					var newKey = keySequence--;
					connectionKeyMappings.Add(connection.ConnectionKey, newKey);
					connection.ConnectionKey = newKey;
					plan.Connections.Add(connection, EImportAction.New);
					isNewConnection = true;
				}
				
				if (existingConnection != null)
				{
					switch (connectionsAction)
					{
						case EImportAction.Replace:
							connectionKeyMappings.Add(connection.ConnectionKey, existingConnection.ConnectionKey);
							connection.ConnectionKey = existingConnection.ConnectionKey;
							plan.Connections.Add(connection, EImportAction.Replace);
							break;
						case EImportAction.New:
							connection.Name = connection.Name + " - duplicate rename " + DateTime.Now;
							NewConnection();
							break;
						case EImportAction.Leave:
						case EImportAction.Skip:
							break;
						default:
							throw new ArgumentOutOfRangeException(nameof(columnValidationsAction), columnValidationsAction, null);
					}
				}
				else
				{
					if (connectionsAction != EImportAction.Skip)
					{
						NewConnection();
					}
				}

				if (tablesAction != EImportAction.Skip)
				{
                    void AddTable(DexihTable table)
                    {
                        var newKey = keySequence--;
                        tableKeyMappings.Add(table.TableKey, newKey);
                        table.TableKey = newKey;
                        table.HubKey = hubKey;
                        table.ConnectionKey = connection.ConnectionKey;

                        if (table.FileFormatKey != null)
                        {
                            if (fileFormatKeyMappings.ContainsKey(table.FileFormatKey.Value))
                            {
                                table.FileFormatKey = fileFormatKeyMappings[table.FileFormatKey.Value];
                            }
                        }

                        foreach (var column in table.DexihTableColumns)
                        {
                            column.TableKey = table.TableKey;
                            column.HubKey = hubKey;
                            newKey = keySequence--;
                            columnKeyMappings.Add(column.ColumnKey, newKey);
                            column.ColumnKey = newKey;

	                        if (column.ColumnValidationKey != null)
	                        {
		                        if (columnValidationKeyMappings.ContainsKey(column.ColumnValidationKey.Value))
		                        {
			                        column.ColumnValidationKey = columnValidationKeyMappings[column.ColumnValidationKey.Value];
		                        }
	                        }
                        }
                        plan.Tables.Add(table, EImportAction.New);
                    }

					if (isNewConnection)
					{
						foreach (var table in tables)
						{
                            AddTable(table);
						}
					}
					else
					{
						var existingTables = await DbContext.DexihTables.Where(tab =>
								tab.HubKey == hubKey && tables.Select(c => c.Name).Contains(tab.Name) && tab.IsValid)
							.ToArrayAsync();

						foreach (var table in tables)
						{
							table.HubKey = hubKey;
							table.ConnectionKey = connection.ConnectionKey;
							if (table.FileFormatKey != null)
							{
								if (fileFormatKeyMappings.ContainsKey(table.FileFormatKey.Value))
								{
									table.FileFormatKey = fileFormatKeyMappings[table.FileFormatKey.Value];
								}
							}

							var existingTable = existingTables.SingleOrDefault(tab => table.Name == tab.Name);
                            if(existingTable == null)
                            {
                                AddTable(table);
                            }
							else
							{
								switch (tablesAction)
								{
									case EImportAction.Replace:
										tableKeyMappings.Add(table.TableKey, existingTable.TableKey);
										table.TableKey = existingTable.TableKey;
										plan.Tables.Add(table, EImportAction.Replace);

										var existingColumns =
											await DbContext.DexihTableColumns.Where(c => c.HubKey == hubKey && c.TableKey == existingTable.TableKey && c.IsValid).ToArrayAsync();

										foreach (var column in table.DexihTableColumns)
										{
											column.HubKey = hubKey;
											column.TableKey = table.TableKey;
											if (column.ColumnValidationKey != null)
											{
												if (columnValidationKeyMappings.ContainsKey(column.ColumnValidationKey.Value))
												{
													column.ColumnValidationKey = columnValidationKeyMappings[column.ColumnValidationKey.Value];
												}
											}
											var existingColumn = existingColumns.SingleOrDefault(c => c.Name == column.Name);
											{
												if (existingColumn != null)
												{
													column.ColumnKey = existingColumn.ColumnKey;
												}
												else
												{
													var newKey = keySequence--;
													columnKeyMappings.Add(column.ColumnKey, newKey);
													column.ColumnKey = newKey;
												}
											}
										}
										break;
									case EImportAction.New:
										var newKey2 = keySequence--;
										tableKeyMappings.Add(table.TableKey, newKey2);
										table.TableKey = newKey2;
							
										foreach (var column in table.DexihTableColumns)
										{
											column.HubKey = hubKey;
											if (column.ColumnValidationKey != null)
											{
												if (columnValidationKeyMappings.ContainsKey(column.ColumnValidationKey.Value))
												{
													column.ColumnValidationKey = columnValidationKeyMappings[column.ColumnValidationKey.Value];
												}
											}
											column.TableKey = table.TableKey;
											var newKey = keySequence--;
											columnKeyMappings.Add(column.ColumnKey, newKey);
											column.ColumnKey = newKey;
										}
										plan.Tables.Add(table, EImportAction.New);
										break;
									case EImportAction.Leave:
									case EImportAction.Skip:
										break;
									default:
										throw new ArgumentOutOfRangeException(nameof(columnValidationsAction), columnValidationsAction, null);
								}
							}
						}
					}
				}
			}
			
			var existingDatalinks =
				await DbContext.DexihDatalinks.Where(cv => cv.HubKey == hubKey && hub.DexihDatalinks.Select(c => c.Name).Contains(cv.Name) && cv.IsValid).ToArrayAsync();


			// loop through the datalinks and reset the datalinkKeys.
			foreach (var datalink in hub.DexihDatalinks)
			{
				datalink.HubKey = hubKey;
				var existingdatalink = existingDatalinks.SingleOrDefault(con => datalink.Name == con.Name);

				if (existingdatalink != null)
				{
					switch (datalinksAction)
					{
						case EImportAction.Replace:
							datalinkKeyMappings.Add(datalink.DatalinkKey, existingdatalink.DatalinkKey);
							datalink.DatalinkKey = existingdatalink.DatalinkKey;
							plan.Datalinks.Add(datalink, EImportAction.Replace);
							break;
						case EImportAction.New:
							var newKey = keySequence--;
							datalinkKeyMappings.Add(datalink.DatalinkKey, newKey);
							datalink.DatalinkKey = newKey;
							datalink.Name = datalink.Name + " - duplicate rename " + DateTime.Now;
							plan.Datalinks.Add(datalink, EImportAction.New);
							break;
						case EImportAction.Leave:
						case EImportAction.Skip:
							break;
						default:
							throw new ArgumentOutOfRangeException(nameof(datalinksAction), datalinksAction, null);
					}
				}
				else
				{
					if (datalinksAction != EImportAction.Skip)
					{
						var newKey = keySequence--;
						datalinkKeyMappings.Add(datalink.DatalinkKey, newKey);
						datalink.DatalinkKey = newKey;
						plan.Datalinks.Add(datalink, EImportAction.New);
					}
				}
			}


			//loop through the datalinks again, and reset the other keys.
			//this requires a second loop as a datalink can reference another datalink.
			foreach (var datalink in plan.Datalinks.Select(c => c.Item))
			{
				var datalinkColumnKeyMapping = new Dictionary<long, long>();

				void ResetDatalinkColumn(DexihDatalinkColumn datalinkColumn)
				{
					if (datalinkColumn != null)
					{
						datalinkColumn.DatalinkTable = null;
						datalinkColumn.DatalinkTableKey = null;
						var newKey = keySequence--;
						datalinkColumnKeyMapping.Add(datalinkColumn.DatalinkColumnKey, newKey);
						datalinkColumn.DatalinkColumnKey = newKey;
					}
				}
				
				// resets columns in datalink table.  Used as a funtion as it is required for the sourcedatalink and joindatalink.
				void ResetDatalinkTable(DexihDatalinkTable datalinkTable)
				{
					if (datalinkTable != null)
					{
						datalinkTable.DatalinkTableKey = 0;
						if (datalinkTable.SourceDatalinkKey != null)
						{
							datalinkTable.SourceDatalinkKey = datalinkKeyMappings.GetValueOrDefault(datalinkTable.SourceDatalinkKey.Value);
						}

						if (datalinkTable.SourceTableKey != null)
						{
							datalinkTable.SourceTableKey = tableKeyMappings.GetValueOrDefault(datalinkTable.SourceTableKey.Value);
						}

						foreach (var column in datalinkTable.DexihDatalinkColumns)
						{
							ResetDatalinkColumn(column);
						}
					}
				}

				datalink.SourceDatalinkTableKey = 0;
				ResetDatalinkTable(datalink.SourceDatalinkTable);
				
				datalink.TargetTableKey = datalink.TargetTableKey == null ? (long?) null :
					tableKeyMappings.GetValueOrDefault(datalink.TargetTableKey.Value);

				if (datalink.AuditConnectionKey != null)
				{
					datalink.AuditConnectionKey = connectionKeyMappings.GetValueOrDefault(datalink.AuditConnectionKey.Value);
				}

				datalink.HubKey = hubKey;

				foreach (var datalinkTransform in datalink.DexihDatalinkTransforms.OrderBy(c=>c.Position))
				{
					datalinkTransform.DatalinkKey = 0;
					datalinkTransform.DatalinkTransformKey = 0;
					datalinkTransform.HubKey = hubKey;

					datalinkTransform.JoinDatalinkTableKey = null;
					ResetDatalinkTable(datalinkTransform.JoinDatalinkTable);

					datalinkTransform.HubKey = hubKey;

					foreach (var item in datalinkTransform.DexihDatalinkTransformItems)
					{
						item.DatalinkTransformItemKey = 0;
						item.HubKey = hubKey;
						
						if (item.JoinDatalinkColumn != null)
						{
							item.JoinDatalinkColumnKey = datalinkColumnKeyMapping.GetValueOrDefault(item.JoinDatalinkColumn.DatalinkColumnKey);
							item.JoinDatalinkColumn = null;
						}
						else
						{
							item.JoinDatalinkColumnKey = null;
						}

						if (item.SourceDatalinkColumn != null)
						{
							item.SourceDatalinkColumnKey = datalinkColumnKeyMapping.GetValueOrDefault(item.SourceDatalinkColumn.DatalinkColumnKey);
							item.SourceDatalinkColumn = null;
						}
						else
						{
							item.SourceDatalinkColumnKey = null;
						}

						foreach (var parameter in item.DexihFunctionParameters)
						{
							parameter.FunctionParameterKey = 0;
							parameter.HubKey = hubKey;
							if (parameter.DatalinkColumnKey != null) parameter.DatalinkColumnKey = 0;
							parameter.DatalinkTransformItemKey = 0;

							if (parameter.Direction == DexihParameterBase.EParameterDirection.Input)
							{
								if (parameter.DatalinkColumn != null)
								{
									parameter.DatalinkColumnKey = datalinkColumnKeyMapping.GetValueOrDefault(parameter.DatalinkColumn.DatalinkColumnKey);
									parameter.DatalinkColumn = null;
								}
								else
								{
									parameter.DatalinkColumnKey = null;
								}
							}
							else
							{
								ResetDatalinkColumn(parameter.DatalinkColumn);
							}
						}

						if(item.TargetDatalinkColumnKey != null)  item.TargetDatalinkColumnKey = 0;
						ResetDatalinkColumn(item.TargetDatalinkColumn);
					}
				}
			}
			
			
			var existingDatajobs =
				await DbContext.DexihDatajobs.Where(cv => cv.HubKey == hubKey && hub.DexihDatajobs.Select(c => c.Name).Contains(cv.Name) && cv.IsValid).ToArrayAsync();

			void AddDatajob(DexihDatajob datajob)
			{
				var newKey = keySequence--;
				datajobKeyMappings.Add(datajob.DatajobKey, newKey);
				datajob.DatajobKey = newKey;
				plan.Datajobs.Add(datajob, EImportAction.New);
			}
			foreach (var datajob in hub.DexihDatajobs)
			{
				datajob.HubKey = hubKey;
				
				var existingdatajob = existingDatajobs.SingleOrDefault(con => datajob.Name == con.Name);

				if (existingdatajob != null)
				{
					switch (datajobsAction)
					{
						case EImportAction.Replace:
							datajobKeyMappings.Add(datajob.DatajobKey, existingdatajob.DatajobKey);
							datajob.DatajobKey = existingdatajob.DatajobKey;
							plan.Datajobs.Add(datajob, EImportAction.Replace);
							break;
						case EImportAction.New:
							datajob.Name = datajob.Name + " - duplicate rename " + DateTime.Now;
							AddDatajob(datajob);
							break;
						case EImportAction.Leave:
						case EImportAction.Skip:
							break;
						default:
							throw new ArgumentOutOfRangeException(nameof(datajobsAction), datajobsAction, null);
					}
				}
				else
				{
					if (datajobsAction != EImportAction.Skip)
					{
						AddDatajob(datajob);
					}
				}

				foreach (var trigger in datajob.DexihTriggers)
				{
					trigger.TriggerKey = 0;
					trigger.HubKey = hubKey;
				}

				var stepKeyMapping = new Dictionary<long, DexihDatalinkStep>();
				
				foreach (var step in datajob.DexihDatalinkSteps)
				{
					var newKey = keySequence--;
					stepKeyMapping.Add(step.DatalinkStepKey, step);
					step.DatalinkStepKey = newKey;
					step.DatalinkKey = datalinkKeyMappings.GetValueOrDefault(step.DatalinkKey);
				}
				
				foreach (var step in datajob.DexihDatalinkSteps)
				{
					foreach (var dep in step.DexihDatalinkDependencies)
					{
						dep.DatalinkDependencyKey = 0;
						dep.DatalinkStepKey = step.DatalinkStepKey;
						dep.DependentDatalinkStepKey = stepKeyMapping.GetValueOrDefault(dep.DependentDatalinkStepKey).DatalinkStepKey;
						dep.DependentDatalinkStep = stepKeyMapping.GetValueOrDefault(dep.DependentDatalinkStepKey);
					}

					step.DexihDatalinkDependentSteps = null;
				}

				if (datajob.AuditConnectionKey != null)
				{
					datajob.AuditConnectionKey = connectionKeyMappings.GetValueOrDefault(datajob.AuditConnectionKey.Value);
				}
			}
			
			return plan;
		}


		/// <summary>
		/// Imports are package into the current repository.
		/// </summary>
		/// <param name="import"></param>
		/// <param name="allowPasswordImport">Allows the import to import passwords/connection strings</param>
		/// <returns></returns>
		public async Task ImportPackage(Import import, bool allowPasswordImport)
		{

			//load all the objects into dictionaries, which can be used to reference them by their key
			
			var hubVariables = import.HubVariables
				.Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
				.ToDictionary(c => c.HubVariableKey, c => c);
            var customFunctions = import.CustomFunctions
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.CustomFunctionKey, c => c);
            var fileFormats = import.FileFormats
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.FileFormatKey, c => c);
            var columnValidations = import.ColumnValidations
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.ColumnValidationKey, c => c);
            var connections = import.Connections
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.ConnectionKey, c => c);
            var tables = import.Tables
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.TableKey, c => c);
            var datalinks = import.Datalinks
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.DatalinkKey, c => c);
            var datajobs = import.Datajobs
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.DatajobKey, c => c);

			// reset all the keys
			foreach (var hubVariable in hubVariables.Values)
			{
				if (hubVariable.HubVariableKey < 0) hubVariable.HubVariableKey = 0;

				if (!allowPasswordImport && hubVariable.IsEncrypted)
				{
					hubVariable.Value = "";
					hubVariable.ValueRaw = "";
				}
			}
           
			// reset all the connection keys, and the connection passwords
            foreach (var connection in connections.Values)
			{
                if (connection.ConnectionKey < 0) connection.ConnectionKey = 0;

				if (!allowPasswordImport)
				{
					connection.ConnectionString = "";
					connection.ConnectionStringRaw = "";
					connection.UseConnectionStringVariable = false;
					connection.Password = "";
					connection.PasswordRaw = "";
					connection.UsePasswordVariable = false;
				}
			}

            foreach(var table in tables.Values)
            {
                if (table.TableKey < 0) table.TableKey = 0;

                table.Connection = connections.GetValueOrDefault(table.ConnectionKey);
                if(table.FileFormatKey != null)
                {
                    table.FileFormat = fileFormats.GetValueOrDefault(table.FileFormatKey.Value);
                }

                foreach(var column in table.DexihTableColumns)
                {
                    if (column.ColumnKey < 0) column.ColumnKey = 0;
                    column.Table = table;
                    if(column.ColumnValidationKey != null)
                    {
                        column.ColumnValidation = columnValidations.GetValueOrDefault(column.ColumnValidationKey.Value);
                    }
                }
            }

			foreach (var columnValidation in columnValidations.Values.Where(c=>c.ColumnValidationKey <0))
			{
				columnValidation.ColumnValidationKey = 0;
			}

			foreach (var datalink in datalinks.Values)
			{
                if(datalink.DatalinkKey < 0) datalink.DatalinkKey = 0;

				if (datalink.AuditConnectionKey != null)
				{
					datalink.AuditConnection = connections.GetValueOrDefault(datalink.AuditConnectionKey.Value);
				}

				datalink.SourceDatalinkTable.SourceDatalink = datalink.SourceDatalinkTable.SourceDatalinkKey == null ? null :
                    datalinks.GetValueOrDefault(datalink.SourceDatalinkTable.SourceDatalinkKey.Value);

                datalink.SourceDatalinkTable.SourceTable = datalink.SourceDatalinkTable.SourceTableKey == null ? null : 
                    tables.GetValueOrDefault(datalink.SourceDatalinkTable.SourceTableKey.Value);

				datalink.TargetTable = datalink.TargetTableKey == null ? null :
                    tables.GetValueOrDefault(datalink.TargetTableKey.Value);

                var datalinkColumns = datalink.SourceDatalinkTable.DexihDatalinkColumns.ToDictionary(c => c.DatalinkColumnKey, c => c);
                foreach(var column in datalink.SourceDatalinkTable.DexihDatalinkColumns)
                {
                    column.DatalinkColumnKey = 0;
                }

				foreach (var datalinkTransform in datalink.DexihDatalinkTransforms.OrderBy(c=>c.Position))
				{
                    datalinkTransform.DatalinkTransformKey = 0;

                    if (datalinkTransform.JoinDatalinkTable != null)
					{
						datalinkTransform.JoinDatalinkTable.SourceDatalink =datalinkTransform.JoinDatalinkTable.SourceDatalinkKey == null ? null :
                            datalinks.GetValueOrDefault(datalinkTransform.JoinDatalinkTable.SourceDatalinkKey.Value);

                        datalinkTransform.JoinDatalinkTable.SourceTable = datalinkTransform.JoinDatalinkTable.SourceTableKey == null ? null :
							tables.GetValueOrDefault(datalinkTransform.JoinDatalinkTable.SourceTableKey.Value);

						foreach (var column in datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns)
						{
							datalinkColumns.Add(column.DatalinkColumnKey, column);
							column.DatalinkColumnKey = 0;
						}
					}

                    foreach(var item in datalinkTransform.DexihDatalinkTransformItems)
                    {
                        item.DatalinkTransformItemKey = 0;
                        item.SourceDatalinkColumn = item.SourceDatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(item.SourceDatalinkColumnKey.Value);
                        item.JoinDatalinkColumn = item.JoinDatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(item.JoinDatalinkColumnKey.Value);

                        if(item.CustomFunctionKey != null)
                        {
                            item.CustomFunction = customFunctions.GetValueOrDefault(item.CustomFunctionKey.Value);
                        }

                        foreach(var param in item.DexihFunctionParameters)
                        {
                            param.FunctionParameterKey = 0;

                            if(param.Direction == DexihParameterBase.EParameterDirection.Input)
                            {
                                param.DatalinkColumn = param.DatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(param.DatalinkColumnKey.Value);
                            }
                            else
                            {
                                if(param.DatalinkColumn != null)
                                {
                                    datalinkColumns.Add(param.DatalinkColumn.DatalinkColumnKey, param.DatalinkColumn);
                                    param.DatalinkColumn.DatalinkColumnKey = 0;
                                }
                            }
                        }

                        if(item.TargetDatalinkColumn != null)
                        {
                            datalinkColumns.Add(item.TargetDatalinkColumn.DatalinkColumnKey, item.TargetDatalinkColumn);
                            item.TargetDatalinkColumn.DatalinkColumnKey = 0;
                        }
                    }
				}
			}

			foreach (var datajob in datajobs.Values)
			{
                if (datajob.DatajobKey < 0) datajob.DatajobKey = 0;

				if (datajob.AuditConnectionKey != null)
				{
					datajob.AuditConnection = connections.GetValueOrDefault(datajob.AuditConnectionKey.Value);
				}

				foreach (var trigger in datajob.DexihTriggers)
				{
					trigger.TriggerKey = 0;
				}

				var steps = datajob.DexihDatalinkSteps.ToDictionary(c => c.DatalinkStepKey, c => c);

				
				foreach (var step in steps.Values)
				{
					step.DatalinkStepKey = 0;
                    step.Datalink = datalinks.GetValueOrDefault(step.DatalinkKey);

					foreach (var dep in step.DexihDatalinkDependencies)
					{
						dep.DatalinkDependencyKey = 0;
						dep.DatalinkStepKey = 0;
						dep.DependentDatalinkStep = steps.GetValueOrDefault(dep.DependentDatalinkStepKey); 
					}
				}
			}

            foreach(var columnValidation in columnValidations.Values.Where(c => c.ColumnValidationKey < 0))
            {
                columnValidation.ColumnValidationKey = 0;
            }

            foreach(var fileFormat in fileFormats.Values.Where(c => c.FileFormatKey < 0))
            {
                fileFormat.FileFormatKey = 0;
            }

            // set all existing datalink object to invalid.
            var datalinkTransforms = datalinks.Values.SelectMany(c => c.DexihDatalinkTransforms).Select(c=>c.DatalinkTransformKey);
			var deletedDatalinkTransforms = DbContext.DexihDatalinkTransforms.Include(c=>c.DexihDatalinkTransformItems).ThenInclude(p=>p.DexihFunctionParameters)
				.Where(t => t.HubKey == import.HubKey &&
				datalinks.Values.Select(d => d.DatalinkKey).Contains(t.DatalinkKey) &&
				!datalinkTransforms.Contains(t.DatalinkTransformKey));

			foreach (var transform in deletedDatalinkTransforms)
			{
				transform.IsValid = false;
				transform.UpdateDate = DateTime.Now;

				foreach (var item in transform.DexihDatalinkTransformItems)
				{
					item.IsValid = false;
					item.UpdateDate = DateTime.Now;

					foreach (var param in item.DexihFunctionParameters)
					{
						param.IsValid = false;
						param.UpdateDate = DateTime.Now;
					}
				}
			}


			var dataLinkSteps = datajobs.Values.SelectMany(c => c.DexihDatalinkSteps).Select(c => c.DatalinkStepKey);
			var deletedSteps = DbContext.DexihDatalinkStep.Where(s =>
             	s.HubKey == import.HubKey &&
				datajobs.Values.Select(d => d.DatajobKey).Contains(s.DatajobKey) && !dataLinkSteps.Contains(s.DatalinkStepKey));
			foreach (var step in deletedSteps)
			{
				step.IsValid = false;
				step.UpdateDate = DateTime.Now;

				foreach (var dep in step.DexihDatalinkDependencies)
				{
					dep.IsValid = false;
					dep.UpdateDate = DateTime.Now;
				}
			}


			// get all columns from the repository that need to be removed.
			var allColumnKeys = tables.Values.SelectMany(t => t.DexihTableColumns).Select(c=>c.ColumnKey).Where(c => c > 0);
			var deletedColumns = DbContext.DexihTableColumns.Where(c =>
			                                                       c.HubKey == import.HubKey &&
				tables.Values.Select(t => t.TableKey).Contains(c.TableKey) && !allColumnKeys.Contains(c.ColumnKey));

			foreach (var column in deletedColumns)
			{
				column.IsValid = false;
				column.UpdateDate = DateTime.Now;
			}

			await DbContext.AddRangeAsync(hubVariables.Values);
			await DbContext.AddRangeAsync(columnValidations.Values);
			await DbContext.AddRangeAsync(fileFormats.Values);
			await DbContext.AddRangeAsync(connections.Values);
			await DbContext.AddRangeAsync(tables.Values);
			await DbContext.AddRangeAsync(datalinks.Values);
			await DbContext.AddRangeAsync(datajobs.Values);

			var entries = DbContext.ChangeTracker.Entries()
				.Where(e => e.State == EntityState.Added || e.State == EntityState.Modified);
			foreach (var entry in entries)
			{
				var item = entry.Entity;
				var properties = item.GetType().GetProperties();
				foreach (var property in properties)
				{
					foreach (var attrib in property.GetCustomAttributes(true))
					{
						if (attrib is CopyCollectionKeyAttribute)
						{
							var value = (long) property.GetValue(item);
							entry.State = value <= 0 ? EntityState.Added : EntityState.Modified;
						}
					}
				}
			}

			await SaveHubChangesAsync(import.HubKey, CancellationToken.None);

		}

    }
	


}

