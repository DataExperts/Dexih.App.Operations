using dexih.functions;
using dexih.repository;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Security.Claims;
using System.Threading.Tasks;
using static dexih.functions.TableColumn;
using Microsoft.Extensions.Logging;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.CopyProperties;
using System.Threading;
using System.Transactions;
using dexih.transforms;
using dexih.transforms.Transforms;
using Microsoft.AspNetCore.Identity;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.operations
{

	public class ImportAction
	{
		public ESharedObjectType ObjectType { get; set; }
		public EImportAction Action { get; set; }
	}

	
	/// <summary>
	/// Provides an interface to retrieve and save to the database repository.
	/// </summary>
	public class RepositoryManager : IDisposable
	{
		
		// Role descriptions used by RoleManager.
		private const string AdministratorRole = "ADMINISTRATOR";
		private const string ManagerRole = "MANAGER";
		private const string UserRole = "USER";
		private const string ViewerRole = "VIEWER";

		private const string RemoteAgentProvider = "dexih-remote"; // name of the token provider used to recognise remote agent calls 
		
		
		private string _systemEncryptionKey;
		private ILogger _logger;
		private readonly UserManager<ApplicationUser> _userManager;

		public DexihRepositoryContext DbContext { get; set; }
		public IMemoryCache MemoryCache { get; set; }
		public readonly Func<Import, Task> HubChange;

		public RepositoryManager(
			 string systemEncryptionKey, 
             DexihRepositoryContext dbContext,
			 UserManager<ApplicationUser> userManager,
			 IMemoryCache memoryCache,
             ILoggerFactory loggerFactory,
			 Func<Import, Task> hubChange
            )
		{
			_systemEncryptionKey = systemEncryptionKey;
			_logger = loggerFactory.CreateLogger("RepositoryManager");
			_userManager = userManager;

			DbContext = dbContext;
			MemoryCache = memoryCache ?? new MemoryCache(new MemoryCacheOptions());
			HubChange = hubChange;
		}

		public RepositoryManager(string systemEncryptionKey, 
			DexihRepositoryContext dbContext,
			UserManager<ApplicationUser> userManager,
			IMemoryCache memoryCache
		)
		{
			_systemEncryptionKey = systemEncryptionKey;
			_userManager = userManager;
			DbContext = dbContext;
			MemoryCache = memoryCache ?? new MemoryCache(new MemoryCacheOptions());
		}
		
		public void Dispose()
		{
			DbContext.Dispose();
		}

		#region User Functions

		[JsonConverter(typeof(StringEnumConverter))]
		public enum ELoginProvider
		{
			Dexih, Google, Microsoft    
		}

		public Task<ApplicationUser> GetUser(ClaimsPrincipal principal)
		{
			var id = _userManager.GetUserId(principal);
			return GetUser(id);
		}

		private async Task AddUserRole(ApplicationUser user)
		{
			var roles = await _userManager.GetRolesAsync(user);
			if (roles.Contains(AdministratorRole)) user.UserRole = ApplicationUser.EUserRole.Administrator;
			else if (roles.Contains(ManagerRole)) user.UserRole = ApplicationUser.EUserRole.Manager;
			else if (roles.Contains(UserRole)) user.UserRole = ApplicationUser.EUserRole.User;
			else if (roles.Contains(ViewerRole)) user.UserRole = ApplicationUser.EUserRole.Viewer;
			else user.UserRole = ApplicationUser.EUserRole.None;
		}
		
		public async Task<ApplicationUser> GetUser(string id)
		{
			var user = await _userManager.FindByIdAsync(id);
			if (user == null)
			{
				throw new RepositoryManagerException($"The user could not be found.");
			}

			await AddUserRole(user);

			return user;
		}

		public async Task<ApplicationUser> GetUserFromEmail(string email)
		{
			var user = await _userManager.FindByEmailAsync(email);

			if (user == null)
			{
				return null;
			}

			await AddUserRole(user);
			return user;
		}

		public async Task<ApplicationUser> GetUserFromLogin(string provider, string providerKey)
		{
			var user = await _userManager.FindByLoginAsync(provider, providerKey);

			if (user == null)
			{
				return null;
			}
			await AddUserRole(user);

			return user;
		}

		/// <summary>
		/// Throws an error when an identity result returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="result"></param>
		private void ThrowIdentityResult(string context, IdentityResult result)
		{
			if (!result.Succeeded)
			{
				throw new RepositoryManagerException($"Could not {context} due to: {string.Join(",", result.Errors.Select(e=>e.Description).ToArray())}");
			}
		}

		public async Task CreateUserAsync(ApplicationUser user, string password = null)
		{
			if (password == null)
			{
				ThrowIdentityResult("create user", await _userManager.CreateAsync(user));
			}
			else
			{
				ThrowIdentityResult("create user", await _userManager.CreateAsync(user, password));
			}

			await CreateUserRoleAsync(user);
		}
		
		public async Task UpdateUserAsync(ApplicationUser user)
		{
			ThrowIdentityResult("update user", await _userManager.UpdateAsync(user));
			await CreateUserRoleAsync(user);
		}

		public async Task AddLoginAsync(ApplicationUser user, ELoginProvider provider, string providerKey)
		{
			var loginInfo = new UserLoginInfo(provider.ToString(), providerKey, provider.ToString());
			ThrowIdentityResult("add user login", await _userManager.AddLoginAsync(user, loginInfo));
		}

		public async Task ConfirmEmailAsync(ApplicationUser user, string code)
		{
			ThrowIdentityResult("confirm email", await _userManager.ConfirmEmailAsync(user, code));
			user.EmailConfirmed = true;
			ThrowIdentityResult("update user", await _userManager.UpdateAsync(user));
		}
		
		public async Task AddPasswordAsync(ApplicationUser user, string password) => ThrowIdentityResult("create user", await _userManager.AddPasswordAsync(user, password));
		public Task<string> GenerateEmailConfirmationTokenAsync(ApplicationUser user) => _userManager.GenerateEmailConfirmationTokenAsync(user);
		public Task<IList<UserLoginInfo>> GetLoginsAsync(ApplicationUser user) => _userManager.GetLoginsAsync(user);
		public async Task RemoveLoginAsync(ApplicationUser user, string provider, string providerKey) => ThrowIdentityResult("remove user login", await _userManager.RemoveLoginAsync(user, provider, providerKey));
		public Task<string> GeneratePasswordResetTokenAsync(ApplicationUser user) => _userManager.GeneratePasswordResetTokenAsync(user);
		public Task<bool> VerifyUserTokenAsync(ApplicationUser user, string remoteAgentId, string token) => _userManager.VerifyUserTokenAsync(user, RemoteAgentProvider, remoteAgentId, token);
		public Task<string> GenerateRemoteUserToken(ApplicationUser user, string remoteAgentId) => _userManager.GenerateUserTokenAsync(user, RemoteAgentProvider, remoteAgentId);
		public async Task ResetPasswordAsync(ApplicationUser user, string code, string password) => ThrowIdentityResult("reset password", await _userManager.ResetPasswordAsync(user, code, password));
		public async Task ChangePasswordAsync(ApplicationUser user, string password, string newPassword) => ThrowIdentityResult("change password", await _userManager.ChangePasswordAsync(user, password, newPassword));
		public async Task DeleteUserAsync(ApplicationUser user) => ThrowIdentityResult("delete user", await _userManager.DeleteAsync(user));

		public async Task CreateUserRoleAsync(ApplicationUser user)
		{
			await _userManager.RemoveFromRolesAsync(user, new[] {AdministratorRole, ManagerRole, ViewerRole, UserRole});

			switch (user.UserRole)
			{
				case ApplicationUser.EUserRole.Administrator:
					await _userManager.AddToRoleAsync(user, AdministratorRole);
					break;
				case ApplicationUser.EUserRole.Manager:
					await _userManager.AddToRoleAsync(user, ManagerRole);
					break;
				case ApplicationUser.EUserRole.User:
					await _userManager.AddToRoleAsync(user, UserRole);
					break;
				case ApplicationUser.EUserRole.Viewer:
					await _userManager.AddToRoleAsync(user, ViewerRole);
					break;
				case ApplicationUser.EUserRole.None:
					break;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		
		
		#endregion
		
		#region Hub Functions

		/// <summary>
		/// clears the cache for any permissions the user has.
		/// </summary>
		/// <param name="userId"></param>
		public void ResetUserCache(string userId)
		{
			MemoryCache.Remove(CacheKeys.UserHubs((userId)));
		}

		/// <summary>
		/// clears the hub cache.
		/// </summary>
		/// <param name="hubKey"></param>
		public void ResetHubCache(long hubKey)
		{
			MemoryCache.Remove($"HUB-{hubKey}");
		}

		
		/// <summary>
		/// Clears the cache for any permissions associated with the hub.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <returns></returns>
		public async Task ResetHubPermissions(long hubKey)
		{
			var hubUsers = await GetHubUsers(hubKey);
			foreach (var hubUser in hubUsers)
			{
				ResetUserCache(hubUser.Id);
			}
			
			MemoryCache.Remove(CacheKeys.AdminHubs);
			MemoryCache.Remove(CacheKeys.HubUserIds(hubKey));
			MemoryCache.Remove(CacheKeys.HubUsers(hubKey));
		}
		
		/// <summary>
		/// Retrieves an array containing the hub and all dependencies along with any dependent objects
		/// </summary>
		/// <param name="hubKey"></param>
		/// <returns></returns>
		public Task<DexihHub> GetHub(long hubKey)
		{
			var hubReturn = MemoryCache.GetOrCreateAsync(CacheKeys.Hub((hubKey)), async entry =>
			{
				entry.SlidingExpiration = TimeSpan.FromHours(1);
				var cache = new CacheManager(hubKey, await GetHubEncryptionKey(hubKey));
				var hub = await cache.LoadHub(DbContext);
				return hub;
			});

			return hubReturn;
		}
		
		public async Task<DexihHubVariable[]> GetHubVariables(long hubKey)
		{
			var hubVariables = await DbContext.DexihHubVariables.Where(c => c.HubKey == hubKey && c.IsValid).ToArrayAsync();
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
		/// <param name="user"></param>
		/// <returns></returns>
		public Task<DexihHub[]> GetUserHubs(ApplicationUser user)
		{
			if (user.IsAdmin)
			{
				return MemoryCache.GetOrCreateAsync(CacheKeys.AdminHubs, async entry =>
				{
					entry.SlidingExpiration = TimeSpan.FromMinutes(1);
				
					var hubs = await DbContext.DexihHubs
						.Include(c => c.DexihHubUsers)
						.Include(c => c.DexihRemoteAgentHubs)
						.Where(c => c.IsValid)
						.ToArrayAsync();
					return hubs;
				});
			}
			else
			{
				return MemoryCache.GetOrCreateAsync(CacheKeys.UserHubs(user.Id), async entry =>
				{
					entry.SlidingExpiration = TimeSpan.FromMinutes(1);
				
					var hubKeys = await DbContext.DexihHubUser
						.Where(c => c.UserId == user.Id && 
						            (c.Permission == DexihHubUser.EPermission.FullReader || c.Permission == DexihHubUser.EPermission.User || c.Permission == DexihHubUser.EPermission.Owner) && c.IsValid)
						.Select(c => c.HubKey).ToArrayAsync();
				
					var hubs = await DbContext.DexihHubs
						.Include(c => c.DexihHubUsers)
						.Include(c => c.DexihRemoteAgentHubs)
						.Where(c => hubKeys.Contains(c.HubKey) && c.IsValid).ToArrayAsync();
					return hubs;
				});
				
			}
		}
		
		/// <summary>
		/// Gets a list of hubs the user can access shared data in.
		/// </summary>
		/// <returns></returns>
		public async Task<DexihHub[]> GetSharedHubs(ApplicationUser user)
		{
			// determine the hubs which can be shared data can be accessed from
			if (user.IsAdmin)
			{
				// admin user has access to all hubs
				return await DbContext.DexihHubs.Where(c => c.IsValid).ToArrayAsync();
			}
			else
			{
				if (string.IsNullOrEmpty(user.Id))
				{
					// no user can only see public hubs
					return await DbContext.DexihHubs.Where(c => c.SharedAccess == DexihHub.ESharedAccess.Public && c.IsValid).ToArrayAsync();
				}
				else
				{
					// all hubs the user has reader access to.
					var readerHubKeys = await DbContext.DexihHubUser.Where(c => c.UserId == user.Id && (c.Permission == DexihHubUser.EPermission.FullReader || c.Permission == DexihHubUser.EPermission.User || c.Permission == DexihHubUser.EPermission.Owner || c.Permission == DexihHubUser.EPermission.PublishReader) && c.IsValid).Select(c=>c.HubKey).ToArrayAsync();
					
					// all hubs the user has reader access to, or are public
					return await DbContext.DexihHubs.Where(c => 
						c.IsValid &&
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
		/// <param name="user"></param>
		/// <param name="hubKey"></param>
		/// <returns></returns>
		public async Task<bool> CanAccessSharedObjects(ApplicationUser user, long hubKey)
		{
			// determine the hubs which can be shared data can be accessed from
			if (user.IsAdmin)
			{
				return true;
			}
			else
			{
				if (string.IsNullOrEmpty(user.Id))
				{
					// no user can only see public hubs
					var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.SharedAccess == DexihHub.ESharedAccess.Public && c.IsValid);
					return hub != null;
				}
				else
				{
					// all hubs the user has reader access to.
					var readerHubKey = await DbContext.DexihHubUser.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == user.Id && c.Permission >= DexihHubUser.EPermission.PublishReader && c.IsValid);

					if (readerHubKey != null)
					{
						return true;
					}
					
					// all hubs other public/shared hubs
					var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => 
						c.HubKey == hubKey &&
						(c.IsValid) &&
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
		/// <param name="user">User</param>
		/// <param name="searchString">Search string to restrict</param>
		/// <param name="hubKeys">HubKeys to restrict search to (null/empty will use all available hubs)</param>
		/// <param name="maxResults">Maximum results to return (0 for all).</param>
		/// <returns></returns>
		/// <exception cref="RepositoryException"></exception>
		public async Task<IEnumerable<SharedData>> GetSharedDataIndex(ApplicationUser user, string searchString, long[] hubKeys, int maxResults = 0)
		{
			var availableHubs = await GetSharedHubs(user);

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
				foreach (var table in hub.DexihTables.Where(c => c.IsShared && ( noSearch || c.Name.ToLower().Contains(search))))
				{
					sharedData.Add(new SharedData()
					{
						HubKey = hub.HubKey,
						HubName = hub.Name,
						ObjectKey = table.Key,
						ObjectType = SharedData.EObjectType.Table,
						Name = table.Name,
						LogicalName = table.LogicalName,
						Description = table.Description,
						UpdateDate = table.UpdateDate,
						InputColumns = table.DexihTableColumns.Where(c => c.IsInput).Select(c=>c.ToInputColumn()).ToArray(),
						OutputColumns = table.DexihTableColumns.Select(c => (DexihColumnBase) c).ToArray()
					});

					if (counter++ > maxResults)
					{
						return sharedData;
					}
				}

				foreach (var datalink in hub.DexihDatalinks.Where(c => c.IsShared && ( noSearch || c.Name.ToLower().Contains(search))))
				{
					sharedData.Add(new SharedData()
					{
						HubKey = datalink.HubKey,
						HubName = hub.Name,
						ObjectKey = datalink.Key,
						ObjectType = SharedData.EObjectType.Datalink,
						Name =  datalink.Name,
						LogicalName =  datalink.Name,
						Description =  datalink.Description,
						UpdateDate =  datalink.UpdateDate,
						InputColumns = datalink.SourceDatalinkTable?.DexihDatalinkColumns?.Where(c => c.IsInput).Select(c=>c.ToInputColumn()).ToArray(),
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

	        // use the Import class to generate a list of hub changes that can be invoked by the HubChange event.
	        var import = new Import(hubKey);

	        foreach (var entity in entities)
	        {
		        // other entries.  If the key value <=0 modify the state to added, otherwise modify existing entity.
		        var item = entity.Entity;
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
			        foreach (var attr in property.GetCustomAttributes(true))
			        {
				        // this shouldn't be possible any longer.
				        if (attr is CopyCollectionKeyAttribute)
				        {
					        var value = (long) property.GetValue(item);
					        entity.State = value <= 0 ? EntityState.Added : EntityState.Modified;
				        }

				        // if the isvalid = false, then set the import action to delete.
				        if (attr is CopyIsValidAttribute)
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
			return MemoryCache.GetOrCreateAsync(CacheKeys.HubUserIds(hubKey), async entry =>
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
	            var returnList = MemoryCache.GetOrCreateAsync(CacheKeys.HubUsers(hubKey), async entry =>
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

        public async Task<DexihHub> GetUserHub(long hubKey, ApplicationUser user)
		{
			if (user.IsAdmin)
			{
				return await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.IsValid);
			}
			else
			{
				var hub = await DbContext.DexihHubUser.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == user.Id && c.IsValid);
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

		public async Task<DexihHub> SaveHub(DexihHub hub, ApplicationUser user)
		{
			try
			{
				if (hub.HubKey > 0 && !user.IsAdmin)
				{
					var permission = await ValidateHub(user, hub.HubKey);

					if(permission != DexihHubUser.EPermission.Owner)
					{
						throw new RepositoryException("Only owners of the hub are able to make modifications.");
					}
				}
				
				var sameName = await DbContext.DexihHubs.AnyAsync(c => c.Name == hub.Name && c.HubKey != hub.HubKey && c.IsValid);
				if (sameName)
				{
                    throw new RepositoryManagerException($"A hub with the name {hub.Name} already exists.");
				}

				// no encryption key provide, then create a random one.
				if(string.IsNullOrEmpty(hub.EncryptionKey) && string.IsNullOrEmpty(hub.EncryptionKey))
				{
					hub.EncryptionKey = Dexih.Utils.Crypto.EncryptString.GenerateRandomKey();
				}

				DexihHub dbHub;
				var isNew = false;

				if (hub.HubKey > 0)
				{
					dbHub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hub.HubKey && c.IsValid);
					if (dbHub != null)
					{
						hub.CopyProperties(dbHub, true);
						isNew = false;
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
					isNew = true;
				}
				
                //save the hub to generate a hub key.
                await DbContext.SaveChangesAsync();
				ResetHubCache(hub.HubKey);
				ResetHubPermissions(hub.HubKey);

				// if new hub, then update with current user, and update the quota.
				if (isNew && !user.IsAdmin)
				{
					user.HubQuota--;
					await UpdateUserAsync(user);
					await HubSetUserPermissions(dbHub.HubKey, new[] { user.Id }, DexihHubUser.EPermission.Owner);
				}

				return dbHub;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save hub {hub.Name} failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihHub[]> DeleteHubs(ApplicationUser user, long[] hubKeys)
		{
			try
			{
				var dbHubs = await DbContext.DexihHubs
					.Where(c => hubKeys.Contains(c.HubKey))
					.ToArrayAsync();

				foreach (var dbHub in dbHubs)
				{
					if (!user.IsAdmin)
					{
						var hubUser = await DbContext.DexihHubUser.SingleOrDefaultAsync(c => c.HubKey == dbHub.HubKey && c.UserId == user.Id && c.IsValid);
						if (hubUser == null || hubUser.Permission != DexihHubUser.EPermission.Owner)
						{
							throw new RepositoryManagerException($"Failed to delete the hub with name {dbHub.Name} as user does not have owner permission on this hub.");
						}
					}

					dbHub.IsValid = false;

					ResetHubCache(dbHub.HubKey);
					await ResetHubPermissions(dbHub.HubKey);
				}
				
				ResetUserCache(user.Id);

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
	                ResetUserCache(userId);
                }
	            
	            await ResetHubPermissions(hubKey);

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
		            ResetUserCache(userHub.UserId);
	            }
	            await DbContext.SaveChangesAsync();
	            await ResetHubPermissions(hubKey);
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
		/// <returns></returns>
		/// <exception cref="ApplicationUserException"></exception>
		public Task<DexihHubUser.EPermission> ValidateHub(ApplicationUser user, long hubKey)
		{
			var validate = MemoryCache.GetOrCreateAsync(CacheKeys.UserHubPermission(user.Id, hubKey), async entry =>
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

				if (user.IsAdmin)
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
                        c.Key != connection.Key && 
                        c.IsValid);

				if (sameName != null)
				{
                    throw new RepositoryManagerException($"The name \"{connection.Name}\" already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (connection.Key > 0)
				{
					dbConnection = await DbContext.DexihConnections.SingleOrDefaultAsync(d => d.Key == connection.Key);
					if (dbConnection != null)
					{
						connection.CopyProperties(dbConnection);
					}
					else
					{
                        throw new RepositoryManagerException($"An update was attempted, however the connection_key {connection.Key} no longer exists in the repository.");
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

		public async Task<DexihConnection> DeleteConnection(long hubKey, long connectionKey)
		{
			var connections = await DeleteConnections(hubKey, new long[] {connectionKey});
			return connections[0];
		}

        public async Task<DexihConnection[]> DeleteConnections(long hubKey, long[] connectionKeys)
		{
            try
            {
                var dbConnections = await DbContext.DexihConnections
                    .Where(c => c.HubKey == hubKey && connectionKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync();

                foreach (var connection in dbConnections)
                {
	                connection.IsValid = false;
                }

                var dbtables = await DbContext.DexihTables
	                .Where(c => connectionKeys.Contains(c.ConnectionKey) && c.IsValid).ToArrayAsync();
                
                foreach (var table in dbtables)
                {
	                table.IsValid = false;

	                foreach (var column in table.DexihTableColumns)
	                {
		                column.IsValid = false;
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
                      .SingleOrDefaultAsync(c => c.Key == connectionKey && c.HubKey == hubKey && c.IsValid);

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


		#endregion

		#region Table Functions

		public async Task<DexihTable> SaveTable(long hubKey,DexihTable hubTable, bool includeColumns, bool includeFileFormat = false)
		{
			var tables = await SaveTables(hubKey, new[] {hubTable}, includeColumns, includeFileFormat);
			return tables[0];
		}
		
		public async Task<DexihTable[]> SaveTables(long hubKey, IEnumerable<DexihTable> tables, bool includeColumns, bool includeFileFormat = false)
		{
			try
			{
				var savedTables = new List<DexihTable>();
				foreach (var table in tables)
				{
					DexihTable dbTable;

					//check there are no datajobs with the same name
					var sameName = await DbContext.DexihTables.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.ConnectionKey == table.ConnectionKey && c.Name == table.Name && c.Key != table.Key && c.IsValid);
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
						var dbFileFormat = await DbContext.DexihFileFormats.SingleOrDefaultAsync(f => f.HubKey == hubKey && f.Key == table.FileFormat.Key);
						if (dbFileFormat == null)
						{
							table.EntityStatus.Message = $"The table could not be saved as the table contains the fileformat {table.FileFormat.Key} that no longer exists in the repository.";
							table.EntityStatus.LastStatus = EntityStatus.EStatus.Error;
                            throw new RepositoryManagerException(table.EntityStatus.Message);
                        }

                        table.FileFormat.CopyProperties(dbFileFormat, true);
						table.FileFormat = dbFileFormat;
					}

					var dbConnection = DbContext.DexihConnections.SingleOrDefault(c => c.HubKey == hubKey && c.Key == table.ConnectionKey);
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

					if (table.Key <= 0)
					{
						dbTable = new DexihTable();
						table.CopyProperties(dbTable, false);
						DbContext.DexihTables.Add(dbTable);
						savedTables.Add(dbTable);
					}
					else
					{
						dbTable = await GetTable(hubKey, table.Key, true);
						
						if (dbTable == null)
						{
							dbTable = new DexihTable();
							table.CopyProperties(dbTable, false);
							DbContext.DexihTables.Add(dbTable);
							savedTables.Add(dbTable);
						}
						else
						{
//							if (dbTable.FileFormat?.FileFormatKey == table.FileFormat?.FileFormatKey)
//							{
//								table.FileFormat = dbTable.FileFormat;
//							}

							table.CopyProperties(dbTable, false);
							dbTable.UpdateDate = DateTime.Now; // change update date to force table to become modified entity.
							savedTables.Add(dbTable);
						}
					}
					
					void SetTableKeys(IEnumerable<DexihTableColumn> columns)
					{
						foreach (var col in columns)
						{
							col.TableKey = null;
							if(col.ChildColumns != null && col.ChildColumns.Count > 0) SetTableKeys(col.ChildColumns);
						}
						
					}

					// set all table keys in child columns to null.
					// this is a workaround as the copyProperties cascades the tableKeys to the child columns.
					foreach (var col in dbTable.DexihTableColumns.Where(c=>c.ChildColumns != null && c.ChildColumns.Any()))
					{
						SetTableKeys(col.ChildColumns);
					}

					dbTable.IsValid = true;
					dbTable.HubKey = hubKey;
				}

				// remove any change tracking on the file format, to avoid an attempted re-save.
//				var entities = DbContext.ChangeTracker.Entries().Where(x => (
//						x.Entity is DexihFileFormat ||
//                        x.Entity is DexihColumnValidation
//					) && (x.State == EntityState.Added || x.State == EntityState.Modified));
//				entities.Select(c => { c.State = EntityState.Unchanged; return c; }).ToList();

				await SaveHubChangesAsync(hubKey);
				return savedTables.ToArray();
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save tables failed.  {ex.Message}", ex);
            }
        }

		public async Task<DexihTable> DeleteTable(long hubKey, long tableKey)
		{
			var tables = await DeleteTables(hubKey, new long[] {tableKey});
			return tables[0];
		}


        public async Task<DexihTable[]> DeleteTables(long hubKey, long[] tableKeys)
		{
            try
            {
                var dbTables = await DbContext.DexihTables
                    .Include(d => d.DexihTableColumns)
                    .Include(f => f.FileFormat)
                    .Where(c => c.HubKey == hubKey && tableKeys.Contains(c.Key) && c.IsValid)
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
                    .Where(c => c.HubKey == hubKey && tableKeys.Contains(c.Key) && c.IsValid)
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
				var dbTables = await DbContext.DexihTables.Where(c => tableKeys.Contains(c.Key) && c.HubKey == hubKey && c.IsValid).ToArrayAsync();

				if (includeColumns)
				{
					await DbContext.DexihTableColumns
						.Where(c => c.TableKey != null && c.IsValid && c.HubKey == hubKey && tableKeys.Contains(c.TableKey.Value))
						.Include(c=>c.ChildColumns)
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
	                .SingleOrDefaultAsync(c => c.Key == tableKey && c.HubKey == hubKey && c.IsValid);

                if (dbTable == null)
                {
                    throw new RepositoryManagerException($"The table with the key {tableKey} could not be found.");
                }

	            if (dbTable.FileFormatKey != null)
	            {
		            dbTable.FileFormat =
			            await DbContext.DexihFileFormats.SingleOrDefaultAsync(
				            c => c.Key == dbTable.FileFormatKey && c.IsValid);
	            }

                if (includeColumns)
                {
                    await DbContext.Entry(dbTable).Collection(a => a.DexihTableColumns).Query()
						.Where(c => c.HubKey == hubKey && c.IsValid && dbTable.Key == c.TableKey)
						.Include(c=>c.ChildColumns).LoadAsync();
                }

                return dbTable;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get table with key {tableKey} failed.  {ex.Message}", ex);
            }
        }

		public async Task<DexihView> GetView(long hubKey, long viewKey)
		{
			try
			{
				var dbView = await DbContext.DexihViews
					.SingleOrDefaultAsync(c => c.Key == viewKey && c.HubKey == hubKey && c.IsValid);

				if (dbView == null)
				{
					throw new RepositoryManagerException($"The view with the key {viewKey} could not be found.");
				}

				return dbView;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get table with key {viewKey} failed.  {ex.Message}", ex);
			}
		}

		
		
		#endregion

        #region Datalink Functions
		

        /// <summary>
        /// This looks at the table attributes and attempts the best strategy.
        /// *** FUTURE: FUNCTION SHOULD BE IMPROVED TO SUGGEST BASED ON PROFILE DATA****
        /// </summary>
        /// <param name="hubTable"></param>
        /// <returns></returns>
        public TransformDelta.EUpdateStrategy GetBestUpdateStrategy(DexihTable hubTable)
		{
            try
            {
                //TODO Improve get best strategy 

	            if (hubTable == null)
		            return TransformDelta.EUpdateStrategy.Reload;
	            else if (hubTable.DexihTableColumns.Count(c => c.DeltaType == TableColumn.EDeltaType.NaturalKey) == 0)
	            {
		            // no natrual key.  Reload is the only choice
		            return TransformDelta.EUpdateStrategy.Reload;
	            }
	            else
	            {
		            if (hubTable.IsVersioned)
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

        public async Task<DexihDatalink[]> SaveDatalinks(long hubKey, DexihDatalink[] hubDatalinks, bool includeTargetTable)
		{
			try
			{
				var savedDatalinks = new List<DexihDatalink>();
				foreach (var datalink in hubDatalinks)
				{
					//check there are no datajobs with the same name
					var sameName = await DbContext.DexihDatalinks.FirstOrDefaultAsync(c =>
						c.HubKey == hubKey && c.Name == datalink.Name && c.Key != datalink.Key &&
						c.IsValid);
					if (sameName != null)
					{
						throw new RepositoryManagerException(
							$"A datalink with the name {datalink.Name} already exists in the repository.");
					}

					DexihDatalink existingDatalink;

					var tables = new Dictionary<long, DexihTable>();
					
					// get the existing datalink so we save over existing data.
					// if there is no existing datalink, create a new one.
					if (datalink.Key <= 0)
					{
						existingDatalink = new DexihDatalink();
						DbContext.Add(existingDatalink);
					}
					else
					{
						var cacheManager = new CacheManager(hubKey, "");
						existingDatalink = await cacheManager.GetDatalink(datalink.Key, DbContext);
					}

					// get columns from the repository instance, and merge the tracked instances into the new one.
					var columns = existingDatalink.GetAllDatalinkColumns();
					var newColumns = datalink.GetAllDatalinkColumns();

					// cache all the target tables.
					if (includeTargetTable)
					{
						foreach (var target in datalink.DexihDatalinkTargets)
						{
							if (target.Table != null)
							{
								target.TableKey = target.Table.Key;
								tables.Add(target.TableKey, target.Table);
							}
						}
					}

					// copy newColumns over existing column instances
					foreach (var newColumn in newColumns.Values)
					{
						if (columns.ContainsKey(newColumn.Key))
						{
							newColumn.CopyProperties(columns[newColumn.Key]);
						}
						else
						{
							columns.Add(newColumn.Key, newColumn);
						}
					}

					// Reset columns ensures only one instance of each column exists.  
					// without this the entity framework tries to insert record twice causing PK violations.
					datalink.CopyProperties(existingDatalink);
					existingDatalink.ResetDatalinkColumns(columns);

//					if (existingDatalink.DatalinkKey == 0 && existingDatalink.TargetTable != null && includeTargetTable)
//					{
//						existingDatalink.TargetTableKey = existingDatalink.TargetTable.TableKey;
//					}

					if (existingDatalink.Key == 0 && includeTargetTable)
					{
						foreach (var target in existingDatalink.DexihDatalinkTargets)
						{
							if (tables.TryGetValue(target.TableKey, out var table))
							{
								target.Table = table;
							}
						}
					}

					if (existingDatalink.SourceDatalinkTable != null)
					{
						existingDatalink.SourceDatalinkTableKey = existingDatalink.SourceDatalinkTable.Key;
					}

					foreach (var transform in existingDatalink.DexihDatalinkTransforms)
					{
						if (transform.JoinDatalinkTable != null)
						{
							transform.JoinDatalinkTableKey = transform.JoinDatalinkTable.Key;
						}
					}

					existingDatalink.UpdateDate = DateTime.Now;
					savedDatalinks.Add(existingDatalink);

//                    if (datalink.DatalinkKey <= 0)
//                    {
//                        var newDatalink = datalink; // .CloneProperties<DexihDatalink>();
//
//						if(includeTargetTable && datalink.TargetTable != null) 
//						{
//							newDatalink.TargetTable = datalink.TargetTable;
//						}
//
//                        newDatalink.ResetDatalinkColumns();
//	                    newDatalink = newDatalink.CloneProperties<DexihDatalink>();
//                        DbContext.Add(newDatalink);
//                        savedDatalinks.Add(newDatalink);
//                    }
//                    else 
//                    {
//	                    var cacheManager = new CacheManager(hubKey, "");
//                        var existingDatalink = await cacheManager.GetDatalink(datalink.DatalinkKey, DbContext);
//
//                        // get columns from the repository instance, and merge the tracked instances into the new one.
//                        var existingColumns = existingDatalink.GetAllDatalinkColumns();
//						var newColumns = datalink.GetAllDatalinkColumns();
//
//						// copy newColumns over existing column instances
//						foreach(var newColumn in newColumns.Values)
//						{
//							if(existingColumns.ContainsKey(newColumn.DatalinkColumnKey))
//							{
//								newColumn.CopyProperties(existingColumns[newColumn.DatalinkColumnKey]);
//							}
//							else
//							{
//								existingColumns.Add(newColumn.DatalinkColumnKey, newColumn);
//							}
//						}
//
//						// Reset columns ensures only one instance of each column exists.  
//						// without this the entity framework tries to insert record twice causing PK violations.
//	                    var newDatalink = datalink;
//	                    newDatalink.ResetDatalinkColumns(existingColumns);
//	                    // newDatalink = newDatalink.CloneProperties<DexihDatalink>();
//	                    newDatalink.CopyProperties(existingDatalink);
//	                    existingDatalink.UpdateDate = DateTime.Now;
//                        savedDatalinks.Add(existingDatalink);
//                    }

					// uncomment to check changes.
//					var modifiedEntries = DbContext.ChangeTracker
//						.Entries()
//						.Where(x => x.State == EntityState.Modified || x.State == EntityState.Added || x.State == EntityState.Deleted)
//						.Select(x => x)
//						.ToList();
					
					await SaveHubChangesAsync(hubKey);

                }

  

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

				if (sourceTableKeys.Length == 0)
				{
					sourceTableKeys = new long[] { 0 };
				}

                long tempColumnKeys = -1;

				var sourceTables = await DbContext.DexihTables.Where(c => c.HubKey == hubKey && sourceTableKeys.Contains(c.Key) && c.IsValid).ToDictionaryAsync(c => c.Key);
				await DbContext.DexihTableColumns.Where(c=> c.HubKey == hubKey && c.TableKey != null && sourceTables.Keys.Contains(c.TableKey.Value) && c.IsValid).Include(c => c.ChildColumns).LoadAsync();
				var targetTable = targetTableKey == null ? null : await DbContext.DexihTables.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.Key == targetTableKey);
				var targetCon = targetConnectionKey == null ? null : await DbContext.DexihConnections.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.Key == targetConnectionKey);

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
						IsQuery = false,
						IsValid = true,
						SourceDatalinkTable = new DexihDatalinkTable()
						{
							SourceType = ESourceType.Table,
							SourceTableKey = sourceTable?.Key,
							SourceTable = sourceTable,
							Name = sourceTable?.Name
						}
					};

					// create a copy of the source table columns, and convert to dexihdatalinkColumn type.
					foreach (var column in sourceTable.DexihTableColumns.OrderBy(c => c.Position))
					{
						var newColumn = new DexihDatalinkColumn();
						column.CopyProperties(newColumn, true);
						newColumn.Key = tempColumnKeys--;
						datalink.SourceDatalinkTable.DexihDatalinkColumns.Add(newColumn);
					}

                    // if there is no target table specified, then create one with the default columns mapped from the source table.
                    if (targetTableKey == null)
                    {
	                    if (targetCon != null)
	                    {
		                    targetTable = await CreateDefaultTargetTable(hubKey, datalinkType, sourceTable, targetTableName, targetCon, addSourceColumns, auditColumns, namingStandards);
		                    var target = new DexihDatalinkTarget {Table = targetTable, IsValid = true};
		                    datalink.DexihDatalinkTargets.Add(target);
		                    datalink.LoadStrategy = TransformWriterTarget.ETransformWriterMethod.Bulk;
	                    }
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
								newColumn.Key = tempColumnKeys--;
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
				
				var changedEntriesCopy = DbContext.ChangeTracker.Entries()
					.Where(e => e.State == EntityState.Added ||
					            e.State == EntityState.Modified ||
					            e.State == EntityState.Deleted)
					.ToList();

				foreach (var entry in changedEntriesCopy)
					entry.State = EntityState.Detached;

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
								   .Where(j => datajobKeys.Contains(j.Key) && j.IsValid && j.HubKey == hubKey).ToArrayAsync();

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

        public async Task<DexihDatajob[]> SaveDatajobs(long hubKey, DexihDatajob[] hubDatajobs)
		{
			try
			{
				var savedDatajobs = new List<DexihDatajob>();
				foreach (var datajob in hubDatajobs)
				{
					if (string.IsNullOrEmpty(datajob.Name))
					{
						throw new RepositoryManagerException($"The datajob requires a name.");
					}
					
					//check there are no datajobs with the same name
					var sameName = await DbContext.DexihDatajobs.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == datajob.Name && c.Key != datajob.Key && c.IsValid);
					if (sameName != null)
					{
                        throw new RepositoryManagerException($"A datajob with the name {datajob.Name} already exists in the repository.");
					}

                    foreach(var step in datajob.DexihDatalinkSteps)
                    {
                        foreach(var dep in step.DexihDatalinkDependencies)
                        {
                            if (dep.Key < 0)
                            {
                                dep.Key = 0;
                            }

                            if (dep.DependentDatalinkStepKey <= 0)
                            {
                                dep.DependentDatalinkStep = datajob.DexihDatalinkSteps.SingleOrDefault(c => c.Key == dep.DependentDatalinkStepKey);
                                dep.DatalinkStepKey = 0;
                            }
                        }
                    }

//                    foreach (var step in datajob.DexihDatalinkSteps)
//                    {
//                        if (step.DatalinkStepKey < 0)
//                        {
//                            step.DatalinkStepKey = 0;
//                        }
//                    }

                    if (datajob.Key <= 0) {
						datajob.Key = 0;
						// var newDatajob = new DexihDatajob();
						// datajob.CopyProperties(newDatajob, false);
						DbContext.DexihDatajobs.Add(datajob);
						savedDatajobs.Add(datajob);
					}
					else
					{
						var originalDatajob = await GetDatajob(hubKey, datajob.Key);

						if(originalDatajob == null)
						{
							datajob.Key = 0;
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
					.Where(c => c.HubKey == hubKey && datajobKeys.Contains(c.Key))
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
		
	   public async Task<DexihRemoteAgent> SaveRemoteAgent(string userId, DexihRemoteAgent hubRemoteAgent)
		{
            try
            {
	            DexihRemoteAgent dbRemoteAgent;
	            
                //if there is a remoteAgentKey, retrieve the record from the database, and copy the properties across.
                if (hubRemoteAgent.RemoteAgentKey > 0)
                {
                    dbRemoteAgent = await DbContext.DexihRemoteAgents.SingleOrDefaultAsync(d => d.RemoteAgentKey == hubRemoteAgent.RemoteAgentKey && d.IsValid);
                    if (dbRemoteAgent != null)
                    {
	                    if (dbRemoteAgent.UserId == userId)
	                    {
		                    hubRemoteAgent.CopyProperties(dbRemoteAgent, true);
		                    dbRemoteAgent.UserId = userId;
	                    }
	                    else
	                    {
		                    throw new RepositoryManagerException("The remote agent could not be updated as the current user is different from the logged in user.  To save with a different user, delete the existing instance, and then save again.");
	                    }
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The remote agent could not be saved as the remote agent contains the remoteagent_key {hubRemoteAgent.RemoteAgentId} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbRemoteAgent = hubRemoteAgent;
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

				// remove any permissions for the hub.
				var hubs = DbContext.DexihRemoteAgentHubs.Where(c => c.RemoteAgentKey == dbItem.RemoteAgentKey);
				foreach(var hub in hubs)
				{
					hub.IsValid = false;
				}
				
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
			return await MemoryCache.GetOrCreateAsync(CacheKeys.RemoteAgentHubs(remoteSettings.AppSettings.RemoteAgentId),
				async entry =>
				{
					entry.SlidingExpiration = TimeSpan.FromMinutes(1); 

					var hubs = await GetUserHubs(remoteSettings.Runtime.User);
			
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
		public async Task<DexihRemoteAgentHub[]> AuthorizedUserRemoteAgentHubs(ApplicationUser user)
		{
			return await MemoryCache.GetOrCreateAsync(CacheKeys.RemoteAgentUserHubs(user.Id),
				async entry =>
				{
					entry.SlidingExpiration = TimeSpan.FromMinutes(1); 

					var hubs = await GetUserHubs(user);
			
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

		public async Task<DexihRemoteAgent[]> GetRemoteAgents(ApplicationUser user)
		{
			var userHubs = (await GetUserHubs(user)).Select(c=>c.HubKey);
			
			var remoteAgents = await DbContext.DexihRemoteAgents.Where(c => 
				c.IsValid && 
				((user.IsAdmin || c.UserId == user.Id) || c.DexihRemoteAgentHubs.Any(d => userHubs.Contains(d.HubKey)))
				).ToArrayAsync();
			return remoteAgents;
		}

        public async Task<DexihRemoteAgentHub> SaveRemoteAgentHub(string userId, long hubKey, DexihRemoteAgentHub hubRemoteAgent)
		{
            try
            {
	            DexihRemoteAgentHub dbRemoteAgent;
	            
                //if there is a remoteAgentKey, retrieve the record from the database, and copy the properties across.
                if (hubRemoteAgent.RemoteAgentHubKey > 0)
                {
                    dbRemoteAgent = await DbContext.DexihRemoteAgentHubs.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.RemoteAgentHubKey == hubRemoteAgent.RemoteAgentHubKey && d.IsValid);
                    if (dbRemoteAgent != null)
                    {
	                    hubRemoteAgent.CopyProperties(dbRemoteAgent, true);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The remote agent could not be saved as the remote agent contains the remoteagent_key {hubRemoteAgent.RemoteAgentHubKey} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbRemoteAgent = hubRemoteAgent;
                    dbRemoteAgent.RemoteAgentHubKey = 0;
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
                DexihTable hubTable = null;

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

					if(await DbContext.DexihTables.AnyAsync(c => c.HubKey == hubKey && c.ConnectionKey == targetConnection.Key && c.Name == tableName && c.IsValid))
					{
						throw new RepositoryManagerException($"The target table could not be created as a table with the name {tableName} already exists.");
					}

                    hubTable = new DexihTable()
                    {
                        HubKey = hubKey,
                        ConnectionKey = targetConnection.Key,
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
					while (await DbContext.DexihTables.AnyAsync(c => c.HubKey == hubKey && c.ConnectionKey == targetConnection.Key && c.Name == newName && c.IsValid))
					{
						newName = $"{baseName}_{count++}";

						if (count > 100)
						{
							throw new RepositoryManagerException("An unexpected nested loop occurred when attempting to create the datalink name.");
						}
					}

                    hubTable = new DexihTable()
                    {
                        HubKey = hubKey,
                        ConnectionKey = targetConnection.Key,
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
			                newColumn.Key = 0;
			                newColumn.MapToTargetColumnProperties();
			                newColumn.Name = newColumn.Name.Replace(" ", " "); //TODO Add better removeUnsupportedCharacters to create target table
			                newColumn.Position = position++;
			                newColumn.IsValid = true;
			                hubTable.DexihTableColumns.Add(newColumn);
		                }
	                }
	                
	                // if there is a auto increment key in the source table, then map it to a column to maintain lineage.
	                if (sourceTable.DexihTableColumns.Count(c => c.DeltaType == TableColumn.EDeltaType.AutoIncrement) > 0)
	                {
		                hubTable.DexihTableColumns.Add(NewDefaultTableColumn("SourceSurrogateKey", namingStandards, hubTable.Name, ETypeCode.Int64, EDeltaType.SourceSurrogateKey, position++));
	                }
                }

	            // add the default for each of the requested auditColumns.
	            foreach (var auditColumn in auditColumns)
	            {
		            var exists = hubTable.DexihTableColumns.FirstOrDefault(c => c.DeltaType == auditColumn && c.IsValid);
		            if (exists == null)
		            {
			            var dataType = TableColumn.GetDeltaDataType(auditColumn);
			            var newColumn =
				            NewDefaultTableColumn(auditColumn.ToString(), namingStandards, hubTable.Name, dataType, auditColumn, position++);

			            // ensure the name is unique.
			            string[] baseName = {newColumn.Name};
			            var version = 1;
			            while (hubTable.DexihTableColumns.FirstOrDefault(c => c.Name == baseName[0] && c.IsValid) != null)
			            {
				            baseName[0] = newColumn.Name + (version++).ToString();
			            }
			            newColumn.Name = baseName[0];
				            
			            hubTable.DexihTableColumns.Add(newColumn);
		            }
	            }

                hubTable.IsValid = true;

                return hubTable;
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
                var dbValidations = await DbContext.DexihColumnValidations
                    .Include(column => column.DexihColumnValidationColumn)
                    .Where(c => c.HubKey == hubKey && columnValidationKeys.Contains(c.Key) && c.IsValid)
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
				var sameName = await DbContext.DexihColumnValidations.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == validation.Name && c.Key != validation.Key && c.IsValid);
				if (sameName != null)
				{
                    throw new RepositoryManagerException($"A column validation with the name {validation.Name} already exists in the repository.");
				}

				//if there is a connectionKey, retrieve the record from the database, and copy the properties across.
				if (validation.Key > 0)
				{
					dbColumnValidation = await DbContext.DexihColumnValidations.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == validation.Key);
					if (dbColumnValidation != null)
					{
						validation.CopyProperties(dbColumnValidation, true);
					}
					else
					{
                        throw new RepositoryManagerException($"The column validation could not be saved as the validation contains the column_validation_key {validation.Key} that no longer exists in the repository.");
					}
				}
				else
				{
					dbColumnValidation = new DexihColumnValidation();
					validation.CopyProperties(dbColumnValidation, true);
					DbContext.DexihColumnValidations.Add(dbColumnValidation);
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
                    .Where(c => c.HubKey == hubKey && customFunctionKeys.Contains(c.Key) && c.IsValid)
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
				var sameName = await DbContext.DexihCustomFunctions.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == function.Name && c.Key != function.Key && c.IsValid);
				if (sameName != null)
				{
                    throw new RepositoryManagerException($"A custom funciton with the name {function.Name} already exists in the hub.");
				}

				//if there is a connectionKey, retrieve the record from the database, and copy the properties across.
				if (function.Key > 0)
				{
					dbFunction = await DbContext.DexihCustomFunctions.Include(c=>c.DexihCustomFunctionParameters).SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == function.Key);
					
					if (dbFunction != null)
					{
						function.CopyProperties(dbFunction, false);
					}
					else
					{
                        throw new RepositoryManagerException($"The custom function could not be saved as the function contains the custom_function_key {function.Key} that no longer exists in the hub.");
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
                var dbFileFormats = await DbContext.DexihFileFormats
                    .Where(c => c.HubKey == hubKey && fileFormatKeys.Contains(c.Key) && c.IsValid)
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
				var sameName = await DbContext.DexihFileFormats.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == fileformat.Name && c.Key != fileformat.Key && c.IsValid);
				if (sameName != null)
				{
                    throw new RepositoryManagerException($"A file format with the name {fileformat.Name} already exists in the repository.");
				}

				//if there is a connectionKey, retrieve the record from the database, and copy the properties across.
				if (fileformat.Key > 0)
				{
					dbFileFormat = await DbContext.DexihFileFormats.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == fileformat.Key);
					if (dbFileFormat != null)
					{
						fileformat.CopyProperties(dbFileFormat, true);
					}
					else
					{
                        throw new RepositoryManagerException($"The file format could not be saved as it contains the file_format_key {fileformat.Key} that no longer exists in the repository.");
					}
				}
				else
				{
					dbFileFormat = new DexihFileFormat();
					fileformat.CopyProperties(dbFileFormat, true);
					DbContext.DexihFileFormats.Add(dbFileFormat);
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
                var dbHubVariables = await DbContext.DexihHubVariables
                    .Where(c => c.HubKey == hubKey && hubVariableKeys.Contains(c.Key) && c.IsValid)
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

        public async Task<DexihHubVariable> SaveHubVariable(long hubKey, DexihHubVariable hubHubVariable)
        {
            try
            {
                DexihHubVariable dbHubHubVariable;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihHubVariables.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == hubHubVariable.Name && c.Key != hubHubVariable.Key && c.IsValid);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A variable with the name {hubHubVariable.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (hubHubVariable.Key > 0)
                {
                    dbHubHubVariable = await DbContext.DexihHubVariables.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == hubHubVariable.Key);
                    if (dbHubHubVariable != null)
                    {
                        hubHubVariable.CopyProperties(dbHubHubVariable, true);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The variable could not be saved as it contains the hub_variable_key {hubHubVariable.Key} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbHubHubVariable = new DexihHubVariable();
                    hubHubVariable.CopyProperties(dbHubHubVariable, true);
                    DbContext.DexihHubVariables.Add(dbHubHubVariable);
                }


                dbHubHubVariable.IsValid = true;

                await SaveHubChangesAsync(hubKey);

                return dbHubHubVariable;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save file format {hubHubVariable.Name} failed.  {ex.Message}", ex);
            }
        }

	   public async Task<DexihView[]> DeleteViews(long hubKey, long[] viewKeys)
        {
            try
            {
                var dbViews = await DbContext.DexihViews
                    .Where(c => c.HubKey == hubKey && viewKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync();

                foreach (var view in dbViews)
                {
	                view.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey);

                return dbViews;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete views failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihView> SaveView(long hubKey, DexihView view)
        {
            try
            {
                DexihView dbView;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihViews.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == view.Name && c.Key != view.Key && c.IsValid);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A view with the name {view.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (view.Key > 0)
                {
                    dbView = await DbContext.DexihViews.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == view.Key);
                    if (dbView != null)
                    {
	                    view.CopyProperties(dbView, true);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The view could not be saved as it contains the view_key {view.Key} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbView = new DexihView();
                    view.CopyProperties(dbView, true);
                    DbContext.DexihViews.Add(dbView);
                }


                dbView.IsValid = true;

                await SaveHubChangesAsync(hubKey);

                return dbView;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save view {view.Name} failed.  {ex.Message}", ex);
            }
        }
        
          public async Task<DexihApi[]> DeleteApis(long hubKey, long[] apiKeys)
        {
            try
            {
                var dbApis = await DbContext.DexihApis
                    .Where(c => c.HubKey == hubKey && apiKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync();

                foreach (var api in dbApis)
                {
	                api.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey);

                return dbApis;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete API's failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihApi> SaveApi(long hubKey, DexihApi api)
        {
            try
            {
	            DexihApi dbApi;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihApis.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == api.Name && c.Key != api.Key && c.IsValid);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A API with the name {api.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (api.Key > 0)
                {
                    dbApi = await DbContext.DexihApis.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == api.Key);
                    if (dbApi != null)
                    {
	                    api.CopyProperties(dbApi, true);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The api could not be saved as it contains the api_key {api.Key} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbApi = new DexihApi();
                    api.CopyProperties(dbApi, true);
                    DbContext.DexihApis.Add(dbApi);
                }

                dbApi.IsValid = true;

                await SaveHubChangesAsync(hubKey);

                return dbApi;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save api {api.Name} failed.  {ex.Message}", ex);
            }
        }
		
		public async Task<DexihDatalinkTest[]> DeleteDatalinkTests(long hubKey, long[] datalinkTestKeys)
        {
            try
            {
                var dbTests = await DbContext.DexihDatalinkTests
                    .Where(c => c.HubKey == hubKey && datalinkTestKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync();

                foreach (var item in dbTests)
                {
	                item.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey);

                return dbTests;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete hub variables failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihDatalinkTest> SaveDatalinkTest(long hubKey, DexihDatalinkTest datalinkTest)
        {
            try
            {
	            DexihDatalinkTest dbDatalinkTest;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihDatalinkTests.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == datalinkTest.Name && c.Key != datalinkTest.Key && c.IsValid);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A test with the name {datalinkTest.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (datalinkTest.Key > 0)
                {
	                var cacheManager = new CacheManager(hubKey, "");
	                dbDatalinkTest = await cacheManager.GetDatalinkTest(datalinkTest.Key, DbContext);
                    if (dbDatalinkTest != null)
                    {
	                    datalinkTest.CopyProperties(dbDatalinkTest, false);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The test could not be saved as it contains the datalink_test_key {datalinkTest.Key} that no longer exists in the repository.");
                    }
                }
                else
                {
	                dbDatalinkTest = new DexihDatalinkTest();
	                datalinkTest.CopyProperties(dbDatalinkTest, false);
                    DbContext.DexihDatalinkTests.Add(dbDatalinkTest);
                }


	            dbDatalinkTest.UpdateDate = DateTime.Now;	            
	            dbDatalinkTest.IsValid = true;

                await SaveHubChangesAsync(hubKey);

                return dbDatalinkTest;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save file format {datalinkTest.Name} failed.  {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Creates a new datalink test with a separate step for each of the specified datalinkKeys.
        /// </summary>
        /// <param name="hubKey"></param>
        /// <param name="hubHub">Reference to the hub cache</param>
        /// <param name="name"></param>
        /// <param name="datalinkKeys">Array containing datalink keys to add to the test.</param>
        /// <param name="auditConnectionKey"></param>
        /// <param name="targetConnectionKey"></param>
        /// <param name="sourceConnectionKey"></param>
        /// <returns></returns>
        public async Task<DexihDatalinkTest> NewDatalinkTest(long hubKey, DexihHub hub, string name, long[] datalinkKeys, long auditConnectionKey, long targetConnectionKey, long sourceConnectionKey)
		{
			var datalinks = hub.DexihDatalinks.Where(c => datalinkKeys.Contains(c.Key)).ToArray();
			
			var datalinkTest = new DexihDatalinkTest
			{
				Name = string.IsNullOrEmpty(name) ? ( datalinks.Count() == 1 ? $"{datalinks[0].Name} tests" : "datalink tests") : name,
				AuditConnectionKey = auditConnectionKey
			};

			var uniqueId = ShortGuid.NewGuid().ToString().Replace("-", "_");

			foreach (var datalink in datalinks)
			{
				//TODO Update datalinktest to allow for multiple target tables.
				
				var targetTable = datalink.DexihDatalinkTargets.Count == 0
					? null
					: hub.GetTableFromKey(datalink.DexihDatalinkTargets.First().TableKey);

				var targetName = targetTable?.Name ?? $"datalink_{datalink.Key}";

				var datalinkStep = new DexihDatalinkTestStep()
				{
					DatalinkKey = datalink.Key,
					Name = $"Test for datalink {datalink.Name}",
					ExpectedConnectionKey = targetConnectionKey,
					ExpectedTableName = $"{targetName}_expected_{uniqueId}",
					ExpectedSchema = "",
					TargetTableName = $"{targetName}_{uniqueId}",
					TargetConnectionKey = targetConnectionKey
				};

				var sourceTables = datalink.GetAllSourceTables(hub);

				foreach (var table in sourceTables)
				{
					var testTable = new DexihDatalinkTestTable()
					{
						TableKey = table.Key,
						SourceConnectionKey = sourceConnectionKey,
						SourceTableName = $"{table.Name}_source_{uniqueId}",
						SourceSchema = "",
						TestConnectionKey = sourceConnectionKey,
						TestTableName = $"{table.Name}_test_{uniqueId}",
						TestSchema = "",
						Action = DexihDatalinkTestTable.ETestTableAction.DropCreateCopy,
					};
					datalinkStep.DexihDatalinkTestTables.Add(testTable);
				}

				datalinkTest.DexihDatalinkTestSteps.Add(datalinkStep);
			}
			
			var modifiedEntries = DbContext.ChangeTracker
				.Entries()
				.Where(x => x.State == EntityState.Modified || x.State == EntityState.Added || x.State == EntityState.Deleted)
				.Select(x => x)
				.ToList();

			DbContext.DexihDatalinkTests.Add(datalinkTest);
			
			await SaveHubChangesAsync(hubKey);

			return datalinkTest;
		}

        private async Task<(Dictionary<long, long> keyMappings, ImportObjects<T> importObjects)> AddImportItems<T>(long hubKey, ICollection<T> items, DbSet<T> dbItems, EImportAction importAction) where T : DexihHubNamedEntity
        {
	        var keyMappings = new Dictionary<long, long>();
	        var importObjects = new ImportObjects<T>();
	        var keySequence = -1;
	        
	        foreach (var item in items)
	        {
		        
		        item.HubKey = hubKey;
		        var matchingItems = dbItems.Where(var => var.HubKey == hubKey && item.Name == var.Name && item.ParentKey == var.ParentKey && var.IsValid);

		        if (matchingItems.Any())
		        {
			        if (matchingItems.Count() > 1)
			        {
				        importAction = EImportAction.Replace;
			        }
			        
			        switch (importAction)
			        {
				        case EImportAction.Replace:
					        keyMappings.Add(item.Key, matchingItems.First().Key);
					        item.Key = matchingItems.First().Key;
					        importObjects.Add(item, EImportAction.Replace);
					        break;
				        case EImportAction.New:
					        var newKey = keySequence--;
					        keyMappings.Add(item.Key, newKey);
					        item.Key = newKey;
					        item.Name = item.Name + " - duplicate rename " + DateTime.Now;
					        importObjects.Add(item, EImportAction.New);
					        break;
				        case EImportAction.Leave:
				        case EImportAction.Skip:
					        break;
				        default:
					        throw new ArgumentOutOfRangeException("ImportAction", importAction, null);
			        }
		        }
		        else
		        {
			        if (importAction != EImportAction.Skip)
			        {
				        var newKey = keySequence--;
				        keyMappings.Add(item.Key, newKey);
				        item.Key = newKey;
				        importObjects.Add(item, EImportAction.New);
			        }
		        }
	        }

	        return (keyMappings, importObjects);
        }
        
        private void UpdateChildItems<T>(long hubKey, ICollection<T> childItems, ICollection<T> existingItems, EImportAction importAction, Dictionary<long, long> mappings, ref int keySequence) where T : DexihHubNamedEntity
        {
	        foreach (var childItem in childItems)
	        {
		        childItem.HubKey = hubKey;
		        var existingItem = existingItems.SingleOrDefault(var => childItem.Name == var.Name);

		        if (existingItem != null)
		        {
			        switch (importAction)
			        {
				        case EImportAction.Replace:
					        childItem.Key = existingItem.Key;
					        mappings.Add(childItem.Key, existingItem.Key);
					        break;
				        case EImportAction.New:
					        var newKey = keySequence--;
					        mappings.Add(childItem.Key, newKey);
					        childItem.Key = newKey;
					        break;
				        case EImportAction.Leave:
				        case EImportAction.Skip:
					        break;
				        default:
					        throw new ArgumentOutOfRangeException("ImportAction", importAction, null);
			        }
		        }
		        else
		        {
			        if (importAction != EImportAction.Skip)
			        {
				        var newKey = keySequence--;
				        mappings.Add(childItem.Key, newKey);
				        childItem.Key = newKey;
			        }
		        }

		        if (importAction == EImportAction.Replace)
		        {
			        var deleteItems =
				        existingItems.Where(c => c.IsValid && !childItems.Select(t => t.Key).Contains(c.Key));
			        foreach (var deleteItem in deleteItems)
			        {
				        deleteItem.IsValid = false;
			        }
		        }
	        }
        }
        
		/// <summary>
		/// Compares an imported hub structure against the database structure, and maps keys and dependent objects together.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="hubHub">Importing Hub</param>
		/// <param name="hubVariableAction"></param>
		/// <param name="connectionsAction"></param>
		/// <param name="tablesAction"></param>
		/// <param name="datalinksAction"></param>
		/// <param name="datajobsAction"></param>
		/// <param name="fileFormatsAction"></param>
		/// <param name="columnValidationsAction"></param>
		/// <returns></returns>
		/// <exception cref="ArgumentOutOfRangeException"></exception>
		public async Task<Import> CreateImportPlan(long hubKey, DexihHub hub, ImportAction[] importActions)
		{
			var keySequence = -1;
			var plan = new Import(hubKey);

			var actions = importActions.ToDictionary(c => c.ObjectType, c => c.Action);
			
			// add all the top level shared objects 
			var hubVariables = await AddImportItems<DexihHubVariable>(hubKey, hub.DexihHubVariables, DbContext.DexihHubVariables, actions[ESharedObjectType.HubVariable]);
			var connections = await AddImportItems<DexihConnection>(hubKey, hub.DexihConnections, DbContext.DexihConnections, actions[ESharedObjectType.Connection]);
			var datajobs = await AddImportItems<DexihDatajob>(hubKey, hub.DexihDatajobs, DbContext.DexihDatajobs, actions[ESharedObjectType.Datajob]);
			var datalinks = await AddImportItems<DexihDatalink>(hubKey, hub.DexihDatalinks, DbContext.DexihDatalinks, actions[ESharedObjectType.Datalink]);
			var columnValidations = await AddImportItems<DexihColumnValidation>(hubKey, hub.DexihColumnValidations, DbContext.DexihColumnValidations, actions[ESharedObjectType.ColumnValidation]);
			var customFunctions = await AddImportItems<DexihCustomFunction>(hubKey, hub.DexihCustomFunctions, DbContext.DexihCustomFunctions, actions[ESharedObjectType.CustomFunction]);
			var fileFormats = await AddImportItems<DexihFileFormat>(hubKey, hub.DexihFileFormats, DbContext.DexihFileFormats, actions[ESharedObjectType.FileFormat]);
			var apis = await AddImportItems<DexihApi>(hubKey, hub.DexihApis, DbContext.DexihApis, actions[ESharedObjectType.Api]);
			var views = await AddImportItems<DexihView>(hubKey, hub.DexihViews, DbContext.DexihViews, actions[ESharedObjectType.View]);
			var datalinkTests = await AddImportItems<DexihDatalinkTest>(hubKey, hub.DexihDatalinkTests, DbContext.DexihDatalinkTests, actions[ESharedObjectType.DatalinkTest]);

			// update the table connection keys to the target connection keys, as these are required updated before matching
			foreach (var table in hub.DexihTables)
			{
				table.ConnectionKey = UpdateConnectionKey(table.ConnectionKey) ?? 0;
			}
			var tables = await AddImportItems<DexihTable>(hubKey, hub.DexihTables, DbContext.DexihTables, actions[ESharedObjectType.Table]);
			
			plan.HubVariables = hubVariables.importObjects;
			plan.Connections = connections.importObjects;
			plan.Datajobs = datajobs.importObjects;
			plan.Datalinks = datalinks.importObjects;
			plan.Tables = tables.importObjects;
			plan.ColumnValidations = columnValidations.importObjects;
			plan.CustomFunctions = customFunctions.importObjects;
			plan.FileFormats = fileFormats.importObjects;
			plan.Apis = apis.importObjects;
			plan.Views = views.importObjects;
			plan.DatalinkTests = datalinkTests.importObjects;

			long? UpdateConnectionKey(long? key)
			{
				if (key != null)
				{
					if (connections.keyMappings.TryGetValue(key.Value, out var newKey))
					{
						return newKey;	
					}
					else
					{
						plan.Warnings.Add($"The connection key {key} does not exist in the package.  This will need to be manually fixed after import.");
						return null;
					}
				}

				return null;
			}
			
			long? UpdateTableKey(long? key)
			{
				if (key != null)
				{
					if (tables.keyMappings.TryGetValue(key.Value, out var newKey))
					{
						return newKey;	
					}
					else
					{
						plan.Warnings.Add($"The table key {key} does not exist in the package.  This will need to be manually fixed after import.");
						return null;
					}
				}

				return null;
			}

			long? UpdateDatalinkKey(long? key)
			{
				if (key != null)
				{
					if (datalinks.keyMappings.TryGetValue(key.Value, out var newKey))
					{
						return newKey;	
					}
					else
					{
						plan.Warnings.Add($"The datalink key {key} does not exist in the package.  This will need to be manually fixed after import.");
						return null;
					}
				}

				return null;
			}
			
			// add the table column mappings
			var tableColumnMappings = new Dictionary<long, long>();
			var dbTableColumns = await DbContext.DexihTableColumns.Where(c => c.TableKey != null && tables.keyMappings.Keys.Contains(c.TableKey.Value) && c.IsValid).ToArrayAsync();
			foreach (var table in tables.importObjects)
			{
				var existingItems = dbTableColumns.Where(c => c.TableKey == table.Item.Key).ToArray();
				UpdateChildItems(hubKey, table.Item.DexihTableColumns, existingItems, table.ImportAction, tableColumnMappings, ref keySequence);

                if (table.Item.FileFormatKey != null)
                {
                    if (fileFormats.keyMappings.TryGetValue(table.Item.FileFormatKey.Value, out var fileFormatKey))
                    {
	                    table.Item.FileFormatKey = fileFormatKey;
                    }
                    else
                    {
                        plan.Warnings.Add($"The table {table.Item.Name} contains a file format with the key {table.Item.FileFormatKey.Value} which does not exist in the package.  This will need to be manually fixed after import.");
                        table.Item.FileFormatKey = null;
                    }
                }

				foreach (var column in table.Item.DexihTableColumns)
				{
					if (column.ColumnValidationKey != null)
					{
						if (columnValidations.keyMappings.TryGetValue(column.ColumnValidationKey.Value, out var columnValidationKey))
						{
							column.ColumnValidationKey = columnValidationKey;
						}
						else
						{
							plan.Warnings.Add($"The column {table.Item.Name}.{column.Name} contains a validation with the key {column.ColumnValidationKey.Value} which does not exist in the package.  This will need to be manually fixed after import.");
							column.ColumnValidationKey = null;
						}
					}
				}
			}
			
			// need to go through the column validations again, now that the table keys are set to reset any lookups
			foreach (var columnValidation in columnValidations.importObjects.Where(c => c.Item.LookupColumnKey != null))
			{
				if (tableColumnMappings.TryGetValue(columnValidation.Item.LookupColumnKey.Value, out var columnKey))
				{
					columnValidation.Item.LookupColumnKey = columnKey;
				}
				else
				{
					plan.Warnings.Add($"The column validation {columnValidation.Item.Name} contains a column lookup with the key {columnValidation.Item.LookupColumnKey} which does not exist in the package.  This will need to be manually fixed after import.");
					columnValidation.Item.LookupColumnKey = null;    
				}
			}

			foreach (var view in views.importObjects.Select(c => c.Item))
			{
				if (view.SourceType == ESourceType.Datalink && view.SourceDatalinkKey != null)
				{
					if (datalinks.keyMappings.TryGetValue(view.SourceDatalinkKey.Value,
						out var sourceDatalinkKey))
					{
						view.SourceDatalinkKey = sourceDatalinkKey;	
					}
					else
					{
						plan.Warnings.Add($"The view {view.Name} contains the source datalink with the key {view.SourceDatalinkKey} which does not exist in the package.  This will need to be manually fixed after import.");
						view.SourceDatalinkKey = null;
					}
				}
				
				if (view.SourceType == ESourceType.Table && view.SourceTableKey != null)
				{
					if (tables.keyMappings.TryGetValue(view.SourceTableKey.Value, out var sourceTableKey))
					{
						view.SourceTableKey = sourceTableKey;
					}
					else
					{
						plan.Warnings.Add($"The view {view.Name} contains the source table with the key {view.SourceTableKey} which does not exist in the package.  This will need to be manually fixed after import.");
						view.SourceTableKey = null;
					}
				}
			}

		
			foreach (var datalinkTest in datalinkTests.importObjects.Select(c => c.Item))
			{
				datalinkTest.AuditConnectionKey = UpdateConnectionKey(datalinkTest.AuditConnectionKey);

				foreach (var step in datalinkTest.DexihDatalinkTestSteps)
				{
					step.DatalinkKey = UpdateDatalinkKey(step.DatalinkKey) ?? default;
					step.ExpectedConnectionKey = UpdateConnectionKey(step.ExpectedConnectionKey) ?? default;
					step.TargetConnectionKey = UpdateConnectionKey(step.TargetConnectionKey) ?? default;
					foreach (var table in step.DexihDatalinkTestTables)
					{
						table.SourceConnectionKey = UpdateConnectionKey(table.SourceConnectionKey) ?? default;
						table.TestConnectionKey = UpdateConnectionKey(table.TestConnectionKey) ?? default;
					}
				}
			}

			foreach (var api in apis.importObjects.Select(c => c.Item))
			{
				api.SourceDatalinkKey = UpdateDatalinkKey(api.SourceDatalinkKey);
				api.SourceTableKey = UpdateTableKey(api.SourceTableKey);
			}

			//loop through the datalinks again, and reset the other keys.
			//this requires a second loop as a datalink can reference another datalink.
			foreach (var datalink in datalinks.importObjects.Select(c => c.Item))
			{
				var datalinkColumnKeyMapping = new Dictionary<long, long>();

				void ResetDatalinkColumn(DexihDatalinkColumn datalinkColumn)
				{
					if (datalinkColumn != null && !datalinkColumnKeyMapping.ContainsKey(datalinkColumn.Key))
					{
						datalinkColumn.DatalinkTable = null;
						datalinkColumn.DatalinkTableKey = null;
						var newKey = keySequence--;
						datalinkColumnKeyMapping.Add(datalinkColumn.Key, newKey);
						datalinkColumn.Key = newKey;
					}
				}
				
				// resets columns in datalink table.  Used as a funtion as it is required for the sourcedatalink and joindatalink.
				void ResetDatalinkTable(DexihDatalinkTable datalinkTable)
				{
					if (datalinkTable != null)
					{
						datalinkTable.Key = 0;
						if (datalinkTable.SourceType == ESourceType.Datalink && datalinkTable.SourceDatalinkKey != null)
						{
							datalinkTable.SourceDatalinkKey = UpdateDatalinkKey(datalinkTable.SourceDatalinkKey);
						}

						if (datalinkTable.SourceType == ESourceType.Table && datalinkTable.SourceTableKey != null)
						{
							datalinkTable.SourceTableKey = UpdateTableKey(datalinkTable.SourceTableKey);
						}

						foreach (var column in datalinkTable.DexihDatalinkColumns)
						{
							ResetDatalinkColumn(column);
						}
					}
				}

				datalink.SourceDatalinkTableKey = 0;
				ResetDatalinkTable(datalink.SourceDatalinkTable);

				// reset the mappings for target tables that have been saved and keys changed.
				var newTargets = new List<DexihDatalinkTarget>();
				foreach (var target in datalink.DexihDatalinkTargets)
				{
					if (tables.keyMappings.TryGetValue(target.TableKey, out var tableKey))
					{
						target.TableKey = tableKey;
						newTargets.Add(target);
					}
					else
					{
						plan.Warnings.Add($"The datalink {datalink.Name} contains the target table with the key {target.TableKey} which does not exist in the package.  This will need to be manually fixed after import.");
					}
				}
				datalink.DexihDatalinkTargets = newTargets;
				datalink.AuditConnectionKey = UpdateConnectionKey(datalink.AuditConnectionKey);
				datalink.HubKey = hubKey;

				foreach (var datalinkTransform in datalink.DexihDatalinkTransforms.OrderBy(c=>c.Position))
				{
					long? datalinkColumnMapping(DexihDatalinkColumn datalinkColumn)
					{
						if (datalinkColumn == null)
						{
							return null;
						}

						if(datalinkColumnKeyMapping.TryGetValue(datalinkColumn.Key, out var datalinkColumnKey))
						{
							return datalinkColumnKey;
						}
							
						ResetDatalinkColumn(datalinkColumn);

						plan.Warnings.Add($"The datalink {datalink.Name} contains the transform {datalinkTransform.Name}, which contains a source column {datalinkColumn.Name}, which does not exist as a previous mapping.  This will need to be manually fixed after import.");
						return null;
					}
					
					datalinkTransform.DatalinkKey = 0;
					datalinkTransform.Key = 0;
					datalinkTransform.HubKey = hubKey;

					datalinkTransform.JoinDatalinkTableKey = null;
					ResetDatalinkTable(datalinkTransform.JoinDatalinkTable);

					datalinkTransform.NodeDatalinkColumnKey = datalinkColumnMapping(datalinkTransform.NodeDatalinkColumn);
					datalinkTransform.NodeDatalinkColumn = null;

					datalinkTransform.JoinSortDatalinkColumnKey = datalinkColumnMapping(datalinkTransform.JoinSortDatalinkColumn);
					datalinkTransform.JoinSortDatalinkColumn = null;

					datalinkTransform.HubKey = hubKey;

					foreach (var item in datalinkTransform.DexihDatalinkTransformItems.OrderBy(c => c.Position))
					{
						item.Key = 0;
						item.HubKey = hubKey;

						item.JoinDatalinkColumnKey = datalinkColumnMapping(item.JoinDatalinkColumn);
						item.JoinDatalinkColumn = null;

						item.SourceDatalinkColumnKey = datalinkColumnMapping(item.SourceDatalinkColumn);
						item.SourceDatalinkColumn = null;

						if(item.TargetDatalinkColumnKey != null)  item.TargetDatalinkColumnKey = 0;
						ResetDatalinkColumn(item.TargetDatalinkColumn);

						foreach (var parameter in item.DexihFunctionParameters)
						{
							parameter.Key = 0;
							parameter.HubKey = hubKey;
							if (parameter.DatalinkColumnKey != null) parameter.DatalinkColumnKey = 0;
							parameter.DatalinkTransformItemKey = 0;

							if (parameter.IsInput())
							{
								parameter.DatalinkColumnKey = datalinkColumnMapping(parameter.DatalinkColumn);
								parameter.DatalinkColumn = null;
							}
							else
							{
								ResetDatalinkColumn(parameter.DatalinkColumn);
							}
							
							if (parameter.ArrayParameters != null)
							{
								foreach (var arrayParam in parameter.ArrayParameters)
								{
									arrayParam.Key = 0;
			                
									if(arrayParam.IsInput())
									{
										arrayParam.DatalinkColumnKey = datalinkColumnMapping(arrayParam.DatalinkColumn);
										arrayParam.DatalinkColumn = null;
									}
									else
									{
										ResetDatalinkColumn(arrayParam.DatalinkColumn);
									}
								}
							}
						}
					}
				}
			}

			foreach (var datajob in datajobs.importObjects.Select(c=> c.Item))
			{
				foreach (var trigger in datajob.DexihTriggers)
				{
					trigger.Key = 0;
					trigger.HubKey = hubKey;
				}

				var stepKeyMapping = new Dictionary<long, DexihDatalinkStep>();

				var newSteps = new HashSet<DexihDatalinkStep>();
				
				foreach (var step in datajob.DexihDatalinkSteps)
				{
					var newKey = keySequence--;
					stepKeyMapping.Add(step.Key, step);
					step.Key = newKey;

					step.DatalinkKey = UpdateDatalinkKey(step.DatalinkKey);
				}
				datajob.DexihDatalinkSteps = newSteps;
				
				foreach (var step in datajob.DexihDatalinkSteps)
				{
					foreach (var dep in step.DexihDatalinkDependencies)
					{
						dep.Key = 0;
						dep.DatalinkStepKey = step.Key;
						dep.DependentDatalinkStepKey = stepKeyMapping.GetValueOrDefault(dep.DependentDatalinkStepKey).Key;
						dep.DependentDatalinkStep = stepKeyMapping.GetValueOrDefault(dep.DependentDatalinkStepKey);
					}

					step.DexihDatalinkDependentSteps = null;
				}

				datajob.AuditConnectionKey = UpdateConnectionKey(datajob.AuditConnectionKey);
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
				.ToDictionary(c => c.Key, c => c);
            var customFunctions = import.CustomFunctions
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.Key, c => c);
            var fileFormats = import.FileFormats
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.Key, c => c);
            var columnValidations = import.ColumnValidations
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.Key, c => c);
            var connections = import.Connections
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.Key, c => c);
            var tables = import.Tables
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.Key, c => c);
            var datalinks = import.Datalinks
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.Key, c => c);
            var datajobs = import.Datajobs
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.Key, c => c);

			// reset all the keys
			foreach (var hubVariable in hubVariables.Values)
			{
				if (hubVariable.Key < 0) hubVariable.Key = 0;

				if (!allowPasswordImport && hubVariable.IsEncrypted)
				{
					hubVariable.Value = "";
					hubVariable.ValueRaw = "";
				}
			}
           
			// reset all the connection keys, and the connection passwords
            foreach (var connection in connections.Values)
			{
                if (connection.Key < 0) connection.Key = 0;

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
                if (table.Key < 0) table.Key = 0;

                table.Connection = connections.GetValueOrDefault(table.ConnectionKey);
                if(table.FileFormatKey != null)
                {
                    table.FileFormat = fileFormats.GetValueOrDefault(table.FileFormatKey.Value);
                }

                foreach(var column in table.DexihTableColumns)
                {
                    if (column.Key < 0) column.Key = 0;
                    column.Table = table;
                    if(column.ColumnValidationKey != null)
                    {
                        column.ColumnValidation = columnValidations.GetValueOrDefault(column.ColumnValidationKey.Value);
                    }
                }
            }

			foreach (var columnValidation in columnValidations.Values.Where(c=>c.Key <0))
			{
				columnValidation.Key = 0;
			}

			foreach (var datalink in datalinks.Values)
			{
                if(datalink.Key < 0) datalink.Key = 0;

				if (datalink.AuditConnectionKey != null)
				{
					datalink.AuditConnection = connections.GetValueOrDefault(datalink.AuditConnectionKey.Value);
				}

				datalink.SourceDatalinkTable.SourceDatalink = datalink.SourceDatalinkTable.SourceDatalinkKey == null ? null :
                    datalinks.GetValueOrDefault(datalink.SourceDatalinkTable.SourceDatalinkKey.Value);

                datalink.SourceDatalinkTable.SourceTable = datalink.SourceDatalinkTable.SourceTableKey == null ? null : 
                    tables.GetValueOrDefault(datalink.SourceDatalinkTable.SourceTableKey.Value);

//				datalink.TargetTable = datalink.TargetTableKey == null ? null :
//                    tables.GetValueOrDefault(datalink.TargetTableKey.Value);

				foreach (var target in datalink.DexihDatalinkTargets)
				{
					target.Table = tables.GetValueOrDefault(target.TableKey);
				}

                var datalinkColumns = datalink.SourceDatalinkTable.DexihDatalinkColumns.ToDictionary(c => c.Key, c => c);
                foreach(var column in datalink.SourceDatalinkTable.DexihDatalinkColumns)
                {
                    column.Key = 0;
                }

				foreach (var datalinkTransform in datalink.DexihDatalinkTransforms.OrderBy(c=>c.Position))
				{
                    datalinkTransform.Key = 0;

                    if (datalinkTransform.JoinDatalinkTable != null)
					{
						datalinkTransform.JoinDatalinkTable.SourceDatalink =datalinkTransform.JoinDatalinkTable.SourceDatalinkKey == null ? null :
                            datalinks.GetValueOrDefault(datalinkTransform.JoinDatalinkTable.SourceDatalinkKey.Value);

                        datalinkTransform.JoinDatalinkTable.SourceTable = datalinkTransform.JoinDatalinkTable.SourceTableKey == null ? null :
							tables.GetValueOrDefault(datalinkTransform.JoinDatalinkTable.SourceTableKey.Value);

						foreach (var column in datalinkTransform.JoinDatalinkTable.DexihDatalinkColumns)
						{
							datalinkColumns.Add(column.Key, column);
							column.Key = 0;
						}
					}

                    datalinkTransform.NodeDatalinkColumn = datalinkTransform.NodeDatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(datalinkTransform.NodeDatalinkColumnKey.Value);
                    datalinkTransform.JoinSortDatalinkColumn = datalinkTransform.JoinSortDatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(datalinkTransform.JoinSortDatalinkColumnKey.Value);
                    
                    foreach(var item in datalinkTransform.DexihDatalinkTransformItems)
                    {
                        item.Key = 0;
                        item.SourceDatalinkColumn = item.SourceDatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(item.SourceDatalinkColumnKey.Value);
                        item.JoinDatalinkColumn = item.JoinDatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(item.JoinDatalinkColumnKey.Value);

                        if(item.CustomFunctionKey != null)
                        {
                            item.CustomFunction = customFunctions.GetValueOrDefault(item.CustomFunctionKey.Value);
                        }

                        foreach(var param in item.DexihFunctionParameters)
                        {
                            param.Key = 0;

                            if(param.IsInput())
                            {
                                param.DatalinkColumn = param.DatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(param.DatalinkColumnKey.Value);
                            }
                            else
                            {
                                if(param.DatalinkColumn != null)
                                {
                                    datalinkColumns.Add(param.DatalinkColumn.Key, param.DatalinkColumn);
                                    param.DatalinkColumn.Key = 0;
                                }
                            }

	                        if (param.ArrayParameters != null)
	                        {
		                        foreach (var arrayParam in param.ArrayParameters)
		                        {
			                        arrayParam.Key = 0;
			                
			                        if(arrayParam.IsInput())
			                        {
				                        arrayParam.DatalinkColumn = arrayParam.DatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(arrayParam.DatalinkColumnKey.Value);
			                        }
			                        else
			                        {
				                        if(arrayParam.DatalinkColumn != null)
				                        {
					                        datalinkColumns.Add(arrayParam.DatalinkColumn.Key, arrayParam.DatalinkColumn);
					                        arrayParam.DatalinkColumn.Key = 0;
				                        }
			                        }
		                        }
	                        }
                        }

                        if(item.TargetDatalinkColumn != null)
                        {
                            datalinkColumns.Add(item.TargetDatalinkColumn.Key, item.TargetDatalinkColumn);
                            item.TargetDatalinkColumn.Key = 0;
                        }
                    }
				}
			}

			foreach (var datajob in datajobs.Values)
			{
                if (datajob.Key < 0) datajob.Key = 0;

				if (datajob.AuditConnectionKey != null)
				{
					datajob.AuditConnection = connections.GetValueOrDefault(datajob.AuditConnectionKey.Value);
				}

				foreach (var trigger in datajob.DexihTriggers)
				{
					trigger.Key = 0;
				}

				var steps = datajob.DexihDatalinkSteps.ToDictionary(c => c.Key, c => c);

				
				foreach (var step in steps.Values)
				{
					step.Key = 0;
                    step.Datalink =  datalinks.GetValueOrDefault(step.DatalinkKey.Value);

					foreach (var dep in step.DexihDatalinkDependencies)
					{
						dep.Key = 0;
						dep.DatalinkStepKey = 0;
						dep.DependentDatalinkStep = steps.GetValueOrDefault(dep.DependentDatalinkStepKey); 
					}
				}
			}

            foreach(var columnValidation in columnValidations.Values.Where(c => c.Key < 0))
            {
                columnValidation.Key = 0;
            }

            foreach(var fileFormat in fileFormats.Values.Where(c => c.Key < 0))
            {
                fileFormat.Key = 0;
            }

            // set all existing datalink object to invalid.
            var datalinkTransforms = datalinks.Values.SelectMany(c => c.DexihDatalinkTransforms).Select(c=>c.Key);
			var deletedDatalinkTransforms = DbContext.DexihDatalinkTransforms.Include(c=>c.DexihDatalinkTransformItems).ThenInclude(p=>p.DexihFunctionParameters)
				.Where(t => t.HubKey == import.HubKey &&
				datalinks.Values.Select(d => d.Key).Contains(t.DatalinkKey) &&
				!datalinkTransforms.Contains(t.Key));

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


			var dataLinkSteps = datajobs.Values.SelectMany(c => c.DexihDatalinkSteps).Select(c => c.Key);
			var deletedSteps = DbContext.DexihDatalinkStep.Where(s =>
             	s.HubKey == import.HubKey &&
				datajobs.Values.Select(d => d.Key).Contains(s.DatajobKey) && !dataLinkSteps.Contains(s.Key));
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
			var allColumnKeys = tables.Values.SelectMany(t => t.DexihTableColumns).Select(c=>c.Key).Where(c => c > 0);
			var deletedColumns = DbContext.DexihTableColumns.Where(c =>
				c.HubKey == import.HubKey &&  c.TableKey != null &&
				tables.Values.Select(t => t.Key).Contains(c.TableKey.Value) && !allColumnKeys.Contains(c.Key)).Include(c=>c.ChildColumns);

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

