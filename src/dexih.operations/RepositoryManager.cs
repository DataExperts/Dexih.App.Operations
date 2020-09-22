using dexih.functions;
using dexih.repository;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using static dexih.functions.TableColumn;
using Microsoft.Extensions.Logging;
using Dexih.Utils.CopyProperties;
using System.Threading;
using dexih.functions.Query;
using dexih.operations.Extensions;
using dexih.transforms;
using dexih.transforms.Transforms;
using Dexih.Utils.DataType;
using Microsoft.AspNetCore.Identity;

namespace dexih.operations
{
	/// <summary>
    /// Provides an interface to retrieve and save to the database repository.
    /// </summary>
    public class RepositoryManager : IDisposable
	{
        #region Events
        public delegate void HubChange(Import import, string[] users);
        public event HubChange OnHubChange;
        #endregion


        // Role descriptions used by RoleManager.
        private const string AdministratorRole = "ADMINISTRATOR";
		private const string ManagerRole = "MANAGER";
		private const string UserRole = "USER";
		private const string ViewerRole = "VIEWER";

		private const string RemoteAgentProvider = "dexih-remote"; // name of the token provider used to recognise remote agent calls 
		private readonly Uri GitHubUri = new Uri("https://api.github.com");

		private readonly ILogger _logger;
		private readonly UserManager<ApplicationUser> _userManager;

		public DexihRepositoryContext DbContext { get; set; }

		private readonly ICacheService _cacheService;
		private readonly IHttpClientFactory _clientFactory;

		public RepositoryManager(DexihRepositoryContext dbContext,
			 UserManager<ApplicationUser> userManager,
			 ICacheService cacheService,
             ILoggerFactory loggerFactory,
			IHttpClientFactory clientFactory)
		{
			_logger = loggerFactory.CreateLogger("RepositoryManager");
			_userManager = userManager;

			DbContext = dbContext;
			_cacheService = cacheService;
			_clientFactory = clientFactory;
		}
		
		public void Dispose()
		{
			DbContext.Dispose();
		}

		#region User Functions

		public Task<ApplicationUser> GetUserAsync(ClaimsPrincipal principal, CancellationToken cancellationToken, bool throwNullUser = true)
		{
			var id = _userManager.GetUserId(principal);
			return GetUserAsync(id, cancellationToken, throwNullUser);
		}

		public Task<ApplicationUser> FindByEmailAsync(string email)
		{
			return _userManager.FindByEmailAsync(email);
		}

		private async Task AddUserRoleAsync(ApplicationUser user, CancellationToken cancellationToken)
		{
			var roles = await _userManager.GetRolesAsync(user);
			if (roles.Contains(AdministratorRole)) user.UserRole = EUserRole.Administrator;
			else if (roles.Contains(ManagerRole)) user.UserRole = EUserRole.Manager;
			else if (roles.Contains(UserRole)) user.UserRole = EUserRole.User;
			else if (roles.Contains(ViewerRole)) user.UserRole = EUserRole.Viewer;
			else user.UserRole = EUserRole.None;
		}
		
		public async Task<ApplicationUser> GetUserAsync(string id, CancellationToken cancellationToken, bool throwNullUser = true)
		{
			var user = await _userManager.FindByIdAsync(id);
			if (user == null)
			{
				if (throwNullUser)
				{
					throw new RepositoryManagerException($"The user could not be found.");
				}
				else
				{
					return null;
				}
			}

			await AddUserRoleAsync(user, cancellationToken);

			return user;
		}

		public async Task<ApplicationUser> GetUserFromLoginAsync(string login, CancellationToken cancellationToken)
		{
			var user = await _userManager.FindByEmailAsync(login);

			if (user == null)
			{
				user = await _userManager.FindByNameAsync(login);

				if (user == null)
				{
					return null;
				}
			}

			await AddUserRoleAsync(user, cancellationToken);
			return user;
		}

		public async Task<UserModel> GetUserModelAsync(string email, CancellationToken cancellationToken)
		{
			var user = await GetUserFromLoginAsync(email, cancellationToken);

			if (user == null)
			{
				return null;
			}

			var userModel = new UserModel();
			user.CopyProperties(userModel, true);
			userModel.Logins = await GetLoginsAsync(user, cancellationToken);
			return userModel;
		}

		public async Task<ApplicationUser> GetUserFromLoginAsync(string provider, string providerKey, CancellationToken cancellationToken)
		{
			var user = await _userManager.FindByLoginAsync(provider, providerKey);

			if (user == null)
			{
				return null;
			}
			await AddUserRoleAsync(user, cancellationToken);

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

		public async Task CreateUserAsync(ApplicationUser user, string password, CancellationToken cancellationToken)
		{
			if (password == null)
			{
				ThrowIdentityResult("create user", await _userManager.CreateAsync(user));
			}
			else
			{
				ThrowIdentityResult("create user", await _userManager.CreateAsync(user, password));
			}

			await CreateUserRoleAsync(user, cancellationToken);
		}
		
		public async Task UpdateUserAsync(ApplicationUser user, CancellationToken cancellationToken)
		{
			ThrowIdentityResult("update user", await _userManager.UpdateAsync(user));
			await CreateUserRoleAsync(user, cancellationToken);
		}

		public async Task AddLoginAsync(ApplicationUser user, ELoginProvider provider, string providerKey, CancellationToken cancellationToken)
		{
			var loginInfo = new UserLoginInfo(provider.ToString(), providerKey, provider.ToString());
			ThrowIdentityResult("add user login", await _userManager.AddLoginAsync(user, loginInfo));
		}

		public async Task ConfirmEmailAsync(ApplicationUser user, string code, CancellationToken cancellationToken)
		{
			ThrowIdentityResult("confirm email", await _userManager.ConfirmEmailAsync(user, code));
			user.EmailConfirmed = true;
			ThrowIdentityResult("update user", await _userManager.UpdateAsync(user));
		}
		
		public async Task AddPasswordAsync(ApplicationUser user, string password, CancellationToken cancellationToken) => ThrowIdentityResult("create user", await _userManager.AddPasswordAsync(user, password));
		public Task<string> GenerateEmailConfirmationTokenAsync(ApplicationUser user, CancellationToken cancellationToken) => _userManager.GenerateEmailConfirmationTokenAsync(user);
		public Task<IList<UserLoginInfo>> GetLoginsAsync(ApplicationUser user, CancellationToken cancellationToken) => _userManager.GetLoginsAsync(user);
		public async Task RemoveLoginAsync(ApplicationUser user, string provider, string providerKey, CancellationToken cancellationToken) => ThrowIdentityResult("remove user login", await _userManager.RemoveLoginAsync(user, provider, providerKey));
		public Task<string> GeneratePasswordResetTokenAsync(ApplicationUser user, CancellationToken cancellationToken) => _userManager.GeneratePasswordResetTokenAsync(user);
		public Task<bool> VerifyUserTokenAsync(ApplicationUser user, string remoteAgentId, string token, CancellationToken cancellationToken) => _userManager.VerifyUserTokenAsync(user, RemoteAgentProvider, remoteAgentId, token);
		public Task<string> GenerateRemoteUserTokenAsync(ApplicationUser user, string remoteAgentId, CancellationToken cancellationToken) => _userManager.GenerateUserTokenAsync(user, RemoteAgentProvider, remoteAgentId);
		public Task<IdentityResult> RemoveRemoteUserTokenAsync(ApplicationUser user, string remoteAgentId, CancellationToken cancellationToken) => _userManager.RemoveAuthenticationTokenAsync(user, RemoteAgentProvider, remoteAgentId);
		public async Task ResetPasswordAsync(ApplicationUser user, string code, string password, CancellationToken cancellationToken) => ThrowIdentityResult("reset password", await _userManager.ResetPasswordAsync(user, code, password));
		public async Task ChangePasswordAsync(ApplicationUser user, string password, string newPassword, CancellationToken cancellationToken) => ThrowIdentityResult("change password", await _userManager.ChangePasswordAsync(user, password, newPassword));
		public async Task DeleteUserAsync(ApplicationUser user, CancellationToken cancellationToken) => ThrowIdentityResult("delete user", await _userManager.DeleteAsync(user));

		public async Task CreateUserRoleAsync(ApplicationUser user, CancellationToken cancellationToken)
		{
			await _userManager.RemoveFromRolesAsync(user, new[] {AdministratorRole, ManagerRole, ViewerRole, UserRole});

			switch (user.UserRole)
			{
				case EUserRole.Administrator:
					await _userManager.AddToRoleAsync(user, AdministratorRole);
					break;
				case EUserRole.Manager:
					await _userManager.AddToRoleAsync(user, ManagerRole);
					break;
				case EUserRole.User:
					await _userManager.AddToRoleAsync(user, UserRole);
					break;
				case EUserRole.Viewer:
					await _userManager.AddToRoleAsync(user, ViewerRole);
					break;
				case EUserRole.None:
					break;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		#endregion

		#region Issues

		public async Task<JsonDocument> GitHubAction(string uri, object data, string token, CancellationToken cancellationToken)
		{
			var json = JsonSerializer.Serialize(data);
			var jsonContent = new StringContent(json, Encoding.UTF8, "application/json");

			var request = new HttpRequestMessage(HttpMethod.Post, new Uri(GitHubUri, uri))
			{
				Content = jsonContent
			};
			request.Headers.UserAgent.Add(new ProductInfoHeaderValue("DexihUI", "1.0"));
			request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
			request.Headers.Authorization = new AuthenticationHeaderValue("Token", token);
			
			var client = _clientFactory.CreateClient();
			var response = await client.SendAsync(request, cancellationToken);

			if (response.IsSuccessStatusCode)
			{
				var jsonDocument = await JsonDocument.ParseAsync(await response.Content.ReadAsStreamAsync(),
					cancellationToken: cancellationToken);
				return jsonDocument;
			}

			throw new RepositoryException($"There was an issue creating the github issue: {response.ReasonPhrase}.");
		}
		
		public async Task<DexihIssue> SaveIssueAsync(DexihIssue issue, string gitHubAccessToken, ApplicationUser user, CancellationToken cancellationToken)
		{
			var gitHubLink = "";

			if (issue.Key > 0)
			{
				var existingIssue = DbContext.DexihIssues.Single(c => c.Key == issue.Key);
				var issueCopy = issue.CloneProperties();
				issueCopy.DexihIssueComments = null;
				issueCopy.CopyProperties(existingIssue);
			}
			else
			{
				if (!issue.IsPrivate && !string.IsNullOrEmpty(gitHubAccessToken))
				{
					var body = new StringBuilder();
					body.AppendLine("Name: " + issue.Name);
					body.AppendLine("Category: " + issue.Category);
					body.AppendLine("Type: " + issue.Type);
					body.AppendLine("Severity: " + issue.Severity);
					if (!string.IsNullOrEmpty(issue.Link))
					{
						body.AppendLine("Link: " + issue.Link);
					}
					body.AppendLine();
					body.AppendLine(issue.Description);

					var gitIssue = new
					{
						title = issue.Name,
						body = body.ToString(),
					};

					var gitRepo = issue.Category switch
					{
						EIssueCategory.Api => "/repos/DataExperts/Dexih.App.UI/issues",
						EIssueCategory.Other => "/repos/DataExperts/Dexih.App.UI/issues",
						EIssueCategory.Web => "/repos/DataExperts/Dexih.App.UI/issues",
						EIssueCategory.Security => "/repos/DataExperts/Dexih.App.UI/issues",
						EIssueCategory.Saving => "/repos/DataExperts/Dexih.App.Operations/issues",
						EIssueCategory.RemoteAgent => "/repos/DataExperts/Dexih.App.Remote/issues",
						EIssueCategory.Datalink => "/repos/DataExperts/dexih.transforms/issues",
						EIssueCategory.Datajob => "/repos/DataExperts/Dexih.App.Remote/issues",
						EIssueCategory.View => "/repos/DataExperts/Dexih.App.Remote/issues",
						EIssueCategory.Dashboard => "/repos/DataExperts/Dexih.App.Remote/issues",
						_ => "/repos/DataExperts/Dexih.App.UI/issues"
					};

					var jsonDocument = await GitHubAction(gitRepo, gitIssue, gitHubAccessToken, cancellationToken);
					gitHubLink = jsonDocument.RootElement.GetProperty("html_url").GetString();
				}

				issue.UserId = user.Id;
				issue.GitHubLink = gitHubLink;
				
				await DbContext.DexihIssues.AddAsync(issue, cancellationToken);
			}

			await DbContext.SaveChangesAsync(cancellationToken);

			return issue;
		}

		public async Task DeleteIssueAsync(long issueKey, ApplicationUser user, CancellationToken cancellationToken)
		{
			var issue = await DbContext.DexihIssues.SingleOrDefaultAsync(c => c.Key == issueKey, cancellationToken);

			if (issue == null)
			{
				throw new RepositoryManagerException($"The issue with the key {issueKey} was not found.");
			}
			
			var canDelete = false;
			if (user.IsAdmin)
			{
				canDelete = true;
			}
			else
			{
				if (issue.UserId == user.Id)
				{
					canDelete = true;
				}
			}

			if (!canDelete)
			{
				throw new RepositoryException("The user cannot delete the issue.  Only the issue originator or an administrator can delete issues.");
			}

			issue.IsValid = false;
			issue.UpdateDate = DateTime.Now;

			await DbContext.SaveChangesAsync(cancellationToken);
		}

		public async Task AddIssueComment(ApplicationUser user, DexihIssueComment issueComment,
			CancellationToken cancellationToken)
		{
			var canComment = false;
			var issue = await DbContext.DexihIssues.SingleOrDefaultAsync(c => c.Key == issueComment.IssueKey, cancellationToken: cancellationToken);
			
			if (issue == null)
			{
				throw new RepositoryManagerException($"The issue with the key {issueComment.IssueKey} was not found.");
			}
			
			if (user.IsAdmin)
			{
				canComment = true;
			}
			else
			{
				if (issue.UserId == user.Id)
				{
					canComment = true;
				}
			}
			
			issue.UpdateDate = DateTime.UtcNow;

			if (!canComment)
			{
				throw new RepositoryException("The user cannot comment on the issue.  Only the issue originator or an administrator can comment.");
			}

			await DbContext.DexihIssueComments.AddAsync(issueComment, cancellationToken);

			await DbContext.SaveChangesAsync(cancellationToken);
		}
		
		public async Task DeleteIssueCommentAsync(long commentKey, ApplicationUser user, CancellationToken cancellationToken)
		{
			var comment = await DbContext.DexihIssueComments.SingleOrDefaultAsync(c => c.Key == commentKey, cancellationToken);
			
			if (comment == null)
			{
				throw new RepositoryManagerException($"The comment with the key {commentKey} was not found.");
			}
			
			var issue = await DbContext.DexihIssues.SingleOrDefaultAsync(c => c.Key == comment.IssueKey, cancellationToken);

			if (issue == null)
			{
				throw new RepositoryManagerException($"The issue with the key {comment.IssueKey} was not found.");
			}
			
			var canDelete = false;
			if (user.IsAdmin)
			{
				canDelete = true;
			}
			else
			{
				if (issue.UserId == user.Id)
				{
					canDelete = true;
				}
			}

			if (!canDelete)
			{
				throw new RepositoryException("The user cannot delete the issue comment.  Only the issue originator or an administrator can delete issues or comments.");
			}

			comment.IsValid = false;
			comment.UpdateDate = DateTime.Now;

			await DbContext.SaveChangesAsync(cancellationToken);
		}

		public async Task<ICollection<ApplicationUser>> GetIssueUsers(DexihIssue issue, CancellationToken cancellationToken)
		{
			var users = new Dictionary<string, ApplicationUser>();
			var user = await GetUserAsync(issue.UserId, cancellationToken);
			if (user != null)
			{
				users.Add(user.Id, user);
			}
			
			foreach(var comment in issue.DexihIssueComments)
			{
				if (!users.ContainsKey(comment.UserId))
				{
					var commentUser = await GetUserAsync(comment.UserId, cancellationToken);
					if (commentUser != null)
					{
						users.Add(commentUser.Id, commentUser);
					}
				}
			}

			return users.Values;
		}

		public async Task<DexihIssue[]> GetUserIssues(ApplicationUser user, CancellationToken cancellationToken)
		{
			DexihIssue[] issues;
			if (user.IsAdmin)
			{
				issues = await DbContext.DexihIssues.Where(c => c.IsValid).ToArrayAsync(cancellationToken);
			}
			else
			{
				issues = await DbContext.DexihIssues.Where(c => c.UserId == user.Id).ToArrayAsync(cancellationToken);	
			}

			if (issues == null)
			{
				return null; 
			}

			var users = new Dictionary<string, string>();
			foreach (var issue in issues)
			{
				issue.UserName = await GetUserName(issue.UserId, users, cancellationToken);

				foreach(var comment in issue.DexihIssueComments)
				{
					comment.UserName = await GetUserName(comment.UserId, users, cancellationToken);
				}
			}

			return issues;
		}
		
		public async Task<DexihIssue> GetIssue(ApplicationUser user, long issueKey, CancellationToken cancellationToken)
		{
			DexihIssue issue;
			if (user.IsAdmin)
			{
				issue = await DbContext.DexihIssues
					.Include(c => c.DexihIssueComments)
					.SingleOrDefaultAsync(c => c.Key == issueKey && c.IsValid, cancellationToken);
			}
			else
			{
				issue = await DbContext.DexihIssues.Include(c => c.DexihIssueComments).SingleOrDefaultAsync(c => c.UserId == user.Id && c.Key == issueKey && c.IsValid, cancellationToken);
			}
			
			if (issue == null)
			{
				throw new CacheManagerException($"The issue with the key {issueKey} was not found.");
			}
			
			var users = new Dictionary<string, string>();
			issue.UserName = await GetUserName(issue.UserId, users, cancellationToken);

			foreach(var comment in issue.DexihIssueComments)
			{
				comment.UserName = await GetUserName(comment.UserId, users, cancellationToken);
			}

			return issue;
		}

		private async Task<string> GetUserName(string userId, Dictionary<string, string> users, CancellationToken cancellationToken)
		{
			if (users.TryGetValue(userId, out var userName))
			{
				return userName;
			}

			var user = await GetUserAsync(userId, cancellationToken);
			if (user != null)
			{
				users.Add(user.Id, user.UserName);
				return user.UserName;
			}
			return null;
		}
		
		#endregion
		
		#region Hub Functions

		/// <summary>
		/// clears the cache for any permissions the user has.
		/// </summary>
		/// <param name="userId"></param>
		/// <param name="cancellationToken"></param>
		public Task ResetUserCacheAsync(string userId, CancellationToken cancellationToken)
		{
			return ResetCacheAsync(CacheKeys.UserHubs(userId), cancellationToken);
		}

		/// <summary>
		/// clears the hub cache.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="cancellationToken"></param>
		public Task ResetHubCacheAsync(long hubKey, CancellationToken cancellationToken)
		{
			return ResetCacheAsync(CacheKeys.Hub(hubKey), cancellationToken);
		}

		/// <summary>
		/// clears the cache for a specific item
		/// </summary>
		/// <param name="key"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public Task ResetCacheAsync(string key, CancellationToken cancellationToken)
		{
			return _cacheService.Reset(key, cancellationToken);
		}


		/// <summary>
		/// Clears the cache for any permissions associated with the hub.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public async Task ResetHubPermissions(long hubKey, CancellationToken cancellationToken)
		{
			var hubUsers = await GetHubUsers(hubKey, cancellationToken);

			var tasks = new List<Task>();
			foreach (var hubUser in hubUsers)
			{
				tasks.Add(ResetUserCacheAsync(hubUser.Id, cancellationToken));
			}
			
			tasks.Add(ResetCacheAsync(CacheKeys.AdminHubs, cancellationToken));
			tasks.Add(ResetCacheAsync(CacheKeys.HubUserIds(hubKey), cancellationToken));
			tasks.Add(ResetCacheAsync(CacheKeys.HubUsers(hubKey), cancellationToken));

			await Task.WhenAll(tasks.ToArray());
		}

		/// <summary>
		/// Retrieves an array containing the hub and all dependencies along with any dependent objects
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public Task<DexihHub> GetHub(long hubKey, CancellationToken cancellationToken)
		{
			var hubReturn = _cacheService.GetOrCreateAsync(CacheKeys.Hub(hubKey), TimeSpan.FromHours(1), async () =>
			{
				var cache = new CacheManager(hubKey, "", _logger);
				var hub = await cache.LoadHub(DbContext);
				return hub;
			}, cancellationToken);

			return hubReturn;
		}
		
		public async Task<DexihHubVariable[]> GetHubVariables(long hubKey, CancellationToken cancellationToken)
		{
			var hubVariables = await DbContext.DexihHubVariables.Where(c => c.HubKey == hubKey && c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);
			return hubVariables;
		}

		public async Task<EPermission> GetHubUserPermission(long hubKey, string userId, CancellationToken cancellationToken)
		{
			var hubUser = await DbContext.DexihHubUser.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == userId && c.IsValid, cancellationToken: cancellationToken);
			if (hubUser == null)
			{
				return EPermission.None;
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
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public Task<DexihHub[]> GetUserHubs(ApplicationUser user, CancellationToken cancellationToken)
		{
			if (user.IsAdmin)
			{
				return _cacheService.GetOrCreateAsync(CacheKeys.AdminHubs, TimeSpan.FromMinutes(1), async () =>
				{
					var hubs = await DbContext.DexihHubs
						.Include(c => c.DexihHubUsers)
						.Include(c => c.DexihRemoteAgentHubs)
						.Where(c => c.IsValid)
						.ToArrayAsync(cancellationToken: cancellationToken);
					return hubs;
				}, cancellationToken);
			}
			else
			{
				return _cacheService.GetOrCreateAsync(CacheKeys.UserHubs(user.Id), TimeSpan.FromMinutes(1),  async () =>
				{
					var hubKeys = await DbContext.DexihHubUser
						.Where(c => c.UserId == user.Id && 
						            (c.Permission == EPermission.FullReader || c.Permission == EPermission.User || c.Permission == EPermission.Owner) && c.IsValid)
						.Select(c => c.HubKey).ToArrayAsync(cancellationToken: cancellationToken);
				
					var hubs = await DbContext.DexihHubs
						.Include(c => c.DexihHubUsers)
						.Include(c => c.DexihRemoteAgentHubs)
						.Where(c => hubKeys.Contains(c.HubKey) && c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);
					return hubs;
				}, cancellationToken);
			}
		}
		
		/// <summary>
		/// Gets a list of hubs the user can access shared data in.
		/// </summary>
		/// <returns></returns>
		public async Task<DexihHub[]> GetSharedHubs(ApplicationUser user, CancellationToken cancellationToken)
		{
			// determine the hubs which can be shared data can be accessed from
			if (user == null)
			{
				return await DbContext.DexihHubs.Where(c => c.SharedAccess == ESharedAccess.Public && c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);
			}
			else if (user.IsAdmin)
			{
				// admin user has access to all hubs
				return await DbContext.DexihHubs.Where(c => c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);
			}
			else
			{
				if (string.IsNullOrEmpty(user.Id))
				{
					// no user can only see public hubs
					return await DbContext.DexihHubs.Where(c => c.SharedAccess == ESharedAccess.Public && c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);
				}

				// all hubs the user has reader access to.
				var readerHubKeys = await DbContext.DexihHubUser.Where
					(c => c.UserId == user.Id && 
					      (c.Permission == EPermission.FullReader || c.Permission == EPermission.User || c.Permission == EPermission.Owner || c.Permission == EPermission.PublishReader) 
					      && c.IsValid).Select(c=>c.HubKey).ToArrayAsync(cancellationToken: cancellationToken);
					
				// all hubs the user has reader access to, or are public
				return await DbContext.DexihHubs.Where(c => 
					c.IsValid &&
					(
						c.SharedAccess == ESharedAccess.Public ||
						c.SharedAccess == ESharedAccess.Registered ||
						readerHubKeys.Contains(c.HubKey)
					)
				).ToArrayAsync(cancellationToken: cancellationToken);
			}
		}

		/// <summary>
		/// Checks is the user can access shared objects in the hub.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="hubKey"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public async Task<bool> CanAccessSharedObjects(ApplicationUser user, long hubKey, CancellationToken cancellationToken)
		{
			// determine the hubs which can be shared data can be accessed from
			if (user == null)
			{
				var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.SharedAccess == ESharedAccess.Public && c.IsValid, cancellationToken: cancellationToken);
				return hub != null;
			}
			else if (user.IsAdmin)
			{
				return true;
			}
			else
			{
				if (string.IsNullOrEmpty(user.Id))
				{
					// no user can only see public hubs
					var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.SharedAccess == ESharedAccess.Public && c.IsValid, cancellationToken: cancellationToken);
					return hub != null;
				}
				else
				{
					// all hubs the user has reader access to.
					var readerHubKey = await DbContext.DexihHubUser.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == user.Id && c.Permission >= EPermission.PublishReader && c.IsValid, cancellationToken: cancellationToken);

					if (readerHubKey != null)
					{
						return true;
					}
					
					// all hubs other public/shared hubs
					var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => 
						c.HubKey == hubKey &&
						c.IsValid &&
						(
							c.SharedAccess == ESharedAccess.Public ||
							c.SharedAccess == ESharedAccess.Registered 
						), cancellationToken: cancellationToken);
					return hub != null;
				}
			}
		}

		public async Task<long> GetSharedListOfValueKey(EDataObjectType objectType, long key, string parameterName,
			CancellationToken cancellationToken)
		{
			switch (objectType)
			{
				case EDataObjectType.Datalink:
					var datalink = await DbContext.DexihDatalinks.SingleOrDefaultAsync(c => c.Key == key && c.IsShared, cancellationToken: cancellationToken);
					if (datalink == null)
					{
						throw new RepositoryManagerException(
							$"The datalink with the key {key} does not exist or is not shared.");
					}

					var parameter = await DbContext.DexihDatalinkParameters.SingleOrDefaultAsync(c =>
						c.DatalinkKey == key && c.Name == parameterName && c.IsValid, cancellationToken: cancellationToken);

					if (parameter != null && parameter.ListOfValuesKey > 0)
					{
						return parameter.ListOfValuesKey.Value;
					}
					
					throw new RepositoryManagerException($"The datalink {datalink.Name} does not have a parameter with name {parameterName} containing a list of values.");
				case EDataObjectType.View:
					var view = await DbContext.DexihViews.SingleOrDefaultAsync(c => c.Key == key && c.IsShared, cancellationToken: cancellationToken);
					if (view == null)
					{
						throw new RepositoryManagerException(
							$"The view with the key {key} does not exist or is not shared.");
					}

					var viewParameter = await DbContext.DexihViewParameters.SingleOrDefaultAsync(c =>
						c.ViewKey == key && c.Name == parameterName && c.IsValid, cancellationToken: cancellationToken);

					if (viewParameter != null && viewParameter.ListOfValuesKey > 0)
					{
						return viewParameter.ListOfValuesKey.Value;
					}
					
					throw new RepositoryManagerException($"The view {view.Name} does not have a parameter with name {parameterName} containing a list of values.");

				case EDataObjectType.Dashboard:
					var dashboard = await DbContext.DexihDashboards.SingleOrDefaultAsync(c => c.Key == key && c.IsShared, cancellationToken: cancellationToken);
					if (dashboard == null)
					{
						throw new RepositoryManagerException($"The view with the key {key} does not exist or is not shared.");
					}

					var dashboardParameter = await DbContext.DexihDashboardParameters.SingleOrDefaultAsync(c =>
						c.DashboardKey == key && c.Name == parameterName && c.IsValid, cancellationToken: cancellationToken);

					if (dashboardParameter != null && dashboardParameter.ListOfValuesKey > 0)
					{
						return dashboardParameter.ListOfValuesKey.Value;
					}
					
					throw new RepositoryManagerException($"The dashboard {dashboard.Name} does not have a parameter with name {parameterName} containing a list of values.");
				case EDataObjectType.DashboardItem:
					var dashboardItem = await DbContext.DexihDashboardItems.SingleOrDefaultAsync(c => c.Key == key && c.Dashboard.IsShared, cancellationToken: cancellationToken);
					if (dashboardItem == null)
					{
						throw new RepositoryManagerException($"The view with the key {key} does not exist or is not shared.");
					}

					var dashboardItemParameter = await DbContext.DexihDashboardItemParameters.SingleOrDefaultAsync(c =>
						c.DashboardItemKey == key && c.Name == parameterName && c.IsValid, cancellationToken: cancellationToken);

					if (dashboardItemParameter != null && dashboardItemParameter.ListOfValuesKey > 0)
					{
						return dashboardItemParameter.ListOfValuesKey.Value;
					}
					
					throw new RepositoryManagerException($"The dashboard item {dashboardItem.Name} does not have a parameter with name {parameterName} containing a list of values.");
			}
			
			throw new RepositoryManagerException($"The {objectType} with key {key} does not contain parameters.");
		}
		
		public async Task<bool> IsObjectShared(EDataObjectType objectType, long key, CancellationToken cancellationToken)
		{
			switch (objectType)
			{
				case EDataObjectType.Table:
					return await DbContext.DexihTables.SingleAsync(c => c.Key == key && c.IsShared, cancellationToken: cancellationToken) != null;
				case EDataObjectType.Datalink:
					return await DbContext.DexihDatalinks.SingleAsync(c => c.Key == key && c.IsShared, cancellationToken: cancellationToken) != null;
				case EDataObjectType.View:
					return await DbContext.DexihViews.SingleAsync(c => c.Key == key && c.IsShared, cancellationToken: cancellationToken) != null;
				case EDataObjectType.Dashboard:
					return await DbContext.DexihDashboards.SingleAsync(c => c.Key == key && c.IsShared, cancellationToken: cancellationToken) != null;
			}

			return false;
		}

		/// <summary>
		/// Returns the object based on the objectType and key.
		/// </summary>
		/// <param name="sharedObjectType"></param>
		/// <param name="key"></param>
		/// <returns>The sharedObject, or null if not found</returns>
		/// <exception cref="ArgumentOutOfRangeException"></exception>
		public async Task<DexihHubNamedEntity> GetObject(ESharedObjectType sharedObjectType, long key)
		{
			switch (sharedObjectType)
			{
				case ESharedObjectType.Connection:
					return await DbContext.DexihConnections.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.Table:
					return await DbContext.DexihTables.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.FileFormat:
					return await DbContext.DexihFileFormats.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.Datalink:
					return await DbContext.DexihDatalinks.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.Datajob:
					return await DbContext.DexihDatajobs.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.ColumnValidation:
					return await DbContext.DexihColumnValidations.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.HubVariable:
					return await DbContext.DexihHubVariables.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.CustomFunction:
					return await DbContext.DexihCustomFunctions.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.DatalinkTest:
					return await DbContext.DexihDatalinkTests.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.View:
					return await DbContext.DexihViews.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.Api:
					return await DbContext.DexihApis.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.Dashboard:
					return await DbContext.DexihDashboards.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.ListOfValues:
					return await DbContext.DexihListOfValues.SingleOrDefaultAsync(c => c.Key == key);
				case ESharedObjectType.Tags:
					return await DbContext.DexihTags.SingleOrDefaultAsync(c => c.Key == key);
				default:
					throw new ArgumentOutOfRangeException(nameof(sharedObjectType), sharedObjectType, null);
			}
		}

		/// <summary>
		/// Gets a list of all the shared datalinks/table available to the user.
		/// </summary>
		/// <param name="user">User</param>
		/// <param name="searchString">Search string to restrict</param>
		/// <param name="hubKeys">HubKeys to restrict search to (null/empty will use all available hubs)</param>
		/// <param name="maxResults">Maximum results to return (0 for all).</param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		/// <exception cref="RepositoryException"></exception>
		public async Task<IEnumerable<SharedData>> GetSharedDataIndex(ApplicationUser user, string searchString, long[] hubKeys, int maxResults, CancellationToken cancellationToken)
		{
			var availableHubs = await GetSharedHubs(user, cancellationToken);

			// check user has access to all the requested hub keys
			if (hubKeys != null && hubKeys.Length > 0)
			{
				foreach (var hubKey in hubKeys)
				{
					if (!availableHubs.Select(c=>c.HubKey).Contains(hubKey))
					{
						throw new RepositoryException($"The user does not have access to the hub with the key {hubKey}");
					}
				}
			}
			else
			{
				// if no hubkeys specified, then user all available.
				hubKeys = availableHubs.Select(c => c.HubKey).ToArray();
			}
			
			var sharedData = new List<SharedData>();
			var nonCachedHubs = new List<DexihHub>();
			
			// get the cached hubs
			foreach(var hub in availableHubs.Where(c => hubKeys.Contains(c.HubKey)))
			{
				var cacheData = await _cacheService.Get<List<SharedData>>(CacheKeys.HubShared(hub.HubKey), cancellationToken);
				
				if(cacheData == null) 
				{
					// if not in cache, remember so we can query database 
					nonCachedHubs.Add(hub);
				}
				else
				{
					sharedData.AddRange(cacheData);
				}
			}

			// get the remaining non-cached hubs
			var nonCachedData = await GetSharedData(nonCachedHubs, cancellationToken);

			// add the non-cached hubs to the cache
			foreach (var hub in nonCachedHubs)
			{
				await _cacheService.GetOrCreateAsync(CacheKeys.HubShared(hub.HubKey),
					TimeSpan.FromHours(1),
					 () =>
					{
						return Task.FromResult(nonCachedData.Where(c => c.HubKey == hub.HubKey));
					}, cancellationToken);
			}
			
			sharedData.AddRange(nonCachedData);
			
			if (string.IsNullOrEmpty(searchString))
			{
				return sharedData.Take(maxResults);
			}
			else
			{
				return sharedData.Where(c =>
					c.Name.IndexOf(searchString, StringComparison.OrdinalIgnoreCase) >= 0 ||
					c.LogicalName.IndexOf(searchString, StringComparison.OrdinalIgnoreCase) >= 0 ||
					c.Description.IndexOf(searchString, StringComparison.OrdinalIgnoreCase) >= 0)
					.Take(maxResults);
			}
		}

		public async Task<SharedData> GetSharedDataObject(ApplicationUser user, long hubKey, EDataObjectType objectType, long objectKey, CancellationToken cancellationToken)
		{
			var canAccess = await CanAccessSharedObjects(user, hubKey, cancellationToken);

			if (!canAccess)
			{
				throw new RepositoryException($"The user does not have access to the hub with the key {hubKey}");
			}
			
			var sharedData = await _cacheService.Get<IEnumerable<SharedData>>(CacheKeys.HubShared(hubKey), cancellationToken);

			// if no shared data for this hub in cache, then lookup from db and add to cache.
			if (sharedData == null)
			{
				var hub = await DbContext.DexihHubs.SingleAsync(c => c.HubKey == hubKey, cancellationToken: cancellationToken);
				sharedData = await GetSharedData(new List<DexihHub>() {hub}, cancellationToken);
				
				await _cacheService.GetOrCreateAsync(CacheKeys.HubShared(hub.HubKey),
					TimeSpan.FromHours(1),
					() => Task.FromResult(sharedData), cancellationToken);
			}

			var sharedObject = sharedData.SingleOrDefault(c =>
				c.ObjectType == objectType && c.ObjectKey == objectKey);

			if (sharedObject == null)
			{
				throw new RepositoryException($"The shared item with the key {objectKey} could not be found.");
			}

			return sharedObject;
		}
		
		private async Task<IEnumerable<SharedData>> GetSharedData(List<DexihHub> hubs, CancellationToken cancellationToken)
		{
			var shared = new List<SharedData>();
			var hubKeys = hubs.Select(c => c.HubKey).ToArray();
			var hubNames = hubs.ToDictionary(c => c.HubKey, c => c.Name);

			var tables = await DbContext.DexihTables.Where(c => hubKeys.Contains(c.HubKey) && c.IsShared && c.IsValid)
				.ToArrayAsync(cancellationToken: cancellationToken);

			var datalinks = await DbContext.DexihDatalinks.Where(c => hubKeys.Contains(c.HubKey) && c.IsShared && c.IsValid)
				.ToArrayAsync(cancellationToken: cancellationToken);
			if (datalinks.Any())
			{
				await DbContext.DexihDatalinkParameters
					.Where(c => c.IsValid && c.Datalink.IsValid && c.Datalink.IsShared &&
					            hubKeys.Contains(c.HubKey))
					.LoadAsync(cancellationToken: cancellationToken);
			}

			var views = await DbContext.DexihViews.Where(c => hubKeys.Contains(c.HubKey) && c.IsShared && c.IsValid)
				.ToArrayAsync(cancellationToken: cancellationToken);
			if (views.Any())
			{
				await DbContext.DexihViewParameters
					.Where(c => c.IsValid && c.View.IsValid && c.View.IsShared &&
					            hubKeys.Contains(c.HubKey))
					.LoadAsync(cancellationToken: cancellationToken);
			}

			var dashboards = await DbContext.DexihDashboards
				.Where(c => hubKeys.Contains(c.HubKey) && c.IsShared && c.IsValid)
				.ToArrayAsync(cancellationToken: cancellationToken);
			if (dashboards.Any())
			{
				await DbContext.DexihDashboardParameters
					.Where(c => c.IsValid && c.Dashboard.IsValid && c.Dashboard.IsShared &&
					            hubKeys.Contains(c.HubKey))
					.LoadAsync(cancellationToken: cancellationToken);
			}

			var dashboardKeys = dashboards.Select(c => c.Key).ToArray();

			var dashboardItems = await DbContext.DexihDashboardItems
				.Where(c => hubKeys.Contains(c.HubKey) && dashboardKeys.Contains(c.DashboardKey) && c.IsValid)
				.ToArrayAsync(cancellationToken: cancellationToken);
			if (dashboardItems.Any())
			{
				await DbContext.DexihDashboardItemParameters
					.Where(c => c.IsValid && c.DashboardItem.IsValid && dashboardKeys.Contains(c.DashboardItem.DashboardKey) &&
					            hubKeys.Contains(c.HubKey))
					.LoadAsync(cancellationToken: cancellationToken);
			}
			
			var apis = await DbContext.DexihApis.Where(c => hubKeys.Contains(c.HubKey) && c.IsShared && c.IsValid)
				.ToArrayAsync(cancellationToken: cancellationToken);
			if (apis.Any())
			{
				await DbContext.DexihApiParameters
					.Where(c => c.IsValid && c.Api.IsValid && c.Api.IsShared && hubKeys.Contains(c.HubKey))
					.LoadAsync(cancellationToken: cancellationToken);
			}

			foreach (var table in tables)
			{
				shared.Add(new SharedData()
				{
					HubKey = table.HubKey,
					HubName = hubNames[table.HubKey],
					ObjectKey = table.Key,
					ObjectType = EDataObjectType.Table,
					Name = table.Name,
					LogicalName = table.LogicalName,
					Description = table.Description,
					UpdateDate = table.UpdateDate,
					Parameters = null,
				});
			}

			foreach (var datalink in datalinks)
			{
				shared.Add(new SharedData()
				{
					HubKey = datalink.HubKey,
					HubName = hubNames[datalink.HubKey],
					ObjectKey = datalink.Key,
					ObjectType = EDataObjectType.Datalink,
					Name = datalink.Name,
					LogicalName = datalink.Name,
					Description = datalink.Description,
					UpdateDate = datalink.UpdateDate,
					Parameters = datalink.Parameters,
				});
			}

			foreach (var view in views)
			{
				shared.Add(new SharedData()
				{
					HubKey = view.HubKey,
					HubName = hubNames[view.HubKey],
					ObjectKey = view.Key,
					ObjectType = EDataObjectType.View,
					Name = view.Name,
					LogicalName = view.Name,
					Description = view.Description,
					UpdateDate = view.UpdateDate,
					Parameters = view.Parameters,
				});
			}

			foreach (var api in apis)
			{
				shared.Add(new SharedData()
				{
					HubKey = api.HubKey,
					HubName = hubNames[api.HubKey],
					ObjectKey = api.Key,
					ObjectType = EDataObjectType.Api,
					Name = api.Name,
					LogicalName = api.Name,
					Description = api.Description,
					UpdateDate = api.UpdateDate,
					Parameters = api.Parameters,
				});
			}

			foreach (var dashboard in dashboards)
			{
				shared.Add(new SharedData()
				{
					HubKey = dashboard.HubKey,
					HubName = hubNames[dashboard.HubKey],
					ObjectKey = dashboard.Key,
					ObjectType = EDataObjectType.Dashboard,
					Name = dashboard.Name,
					LogicalName = dashboard.Name,
					Description = dashboard.Description,
					UpdateDate = dashboard.UpdateDate,
					Parameters = dashboard.Parameters,
				});
			}
			
			foreach (var dashboardItem in dashboardItems)
			{
				shared.Add(new SharedData()
				{
					HubKey = dashboardItem.HubKey,
					HubName = hubNames[dashboardItem.HubKey],
					ObjectKey = dashboardItem.Key,
					ObjectType = EDataObjectType.DashboardItem,
					Name = dashboardItem.Name,
					LogicalName = dashboardItem.Name,
					Description = dashboardItem.Description,
					UpdateDate = dashboardItem.UpdateDate,
					Parameters = dashboardItem.Parameters,
				});
			}

			ESharedObjectType ConvertObjectType(EDataObjectType dataObjectType)
			{
				switch(dataObjectType)
				{
					case EDataObjectType.Table:
						return ESharedObjectType.Table;
					case EDataObjectType.Datalink:
						return ESharedObjectType.Datalink;
					case EDataObjectType.View:
						return ESharedObjectType.View;
					case EDataObjectType.Dashboard:
						return ESharedObjectType.Dashboard;
					case EDataObjectType.DashboardItem:
						return ESharedObjectType.Dashboard;
					case EDataObjectType.Api:
						return ESharedObjectType.Api;
					default:
						throw new ArgumentOutOfRangeException(nameof(dataObjectType), dataObjectType, null);
				}
			}

			var objectKeys = shared.Select(c => c.ObjectKey).ToArray();
			
			// get all tags in one query
			var tags = await DbContext.DexihTagObjects.Include(c => c.DexihTag)
				.Where(c => c.IsValid && objectKeys.Contains(c.ObjectKey))
				.ToHashSetAsync(cancellationToken: cancellationToken);

			foreach (var sharedItem in shared)
			{
				var t = tags.Where(c =>
					c.HubKey == sharedItem.HubKey && c.ObjectKey == sharedItem.ObjectKey &&
					c.ObjectType == ConvertObjectType(sharedItem.ObjectType))
					.Select(c => c.DexihTag).ToArray();

				if (t.Length > 0)
				{
					sharedItem.Tags = t;
				}
			}
			
			return shared;
		}


		/// <summary>
		/// Saves changes in the dbContext, and raises event to report these back to the client.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
        public async Task SaveHubChangesAsync(long hubKey, CancellationToken cancellationToken = default(CancellationToken))
        {
	        var entities = DbContext.ChangeTracker.Entries()
		        .Where(x => x.State == EntityState.Added || 
	                x.State == EntityState.Modified ||
	                x.State == EntityState.Deleted)
		        .ToArray();

	        // use the Import class to generate a list of hub changes that can be invoked by the HubChange event.
	        var import = new Import(hubKey);

	        foreach (var entity in entities)
	        {
		        // if the hub entity is 0, reset all the dependent child keys to zero to ensure
		        // keys from existing objects are not overwritten.
		        if (entity.Entity is DexihHubNamedEntity hubNamedEntity1)
		        {
			        if (hubNamedEntity1.Key <= 0)
			        {
				        hubNamedEntity1.ResetKeys();
			        }
		        }
	        }

	        foreach (var entity in entities)
	        {
		        // other entries.  If the key value <=0 modify the state to added, otherwise modify existing entity.
		        var item = entity.Entity;

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

		        // get original values from database
		        var originalValues = await entity.GetDatabaseValuesAsync(cancellationToken);

		        if (originalValues == null)
		        {
			        if (entity.State == EntityState.Deleted)
			        {
				        throw new RepositoryException($"The entity {entity.GetType()} can not not be deleted, as it does not exist in the repository.");
			        }

			        importAction = EImportAction.New;
			        entity.State = EntityState.Added;
			        if (entity.Entity is DexihHubEntity hubEntity)
			        {
				        hubEntity.HubKey = hubKey;
			        }
		        }
		        else
		        {
			        if (entity.Entity is DexihHubEntity hubEntity)
			        {
				        hubEntity.HubKey = hubKey;

				        // check hubkey hasn't changed since original.  This could impact an entity in another hub and causes an immediate stop.
				        if (!Equals(originalValues[nameof(DexihHubEntity.HubKey)], hubEntity.HubKey))
				        {
					        if (hubEntity is DexihHubNamedEntity hubNamedEntity2)
					        {
						        throw new SecurityException(
							        $"The hubKey on the original entity and the updated entity have changed.  The entity was type:{hubEntity.GetType()}, key: {hubNamedEntity2.Key}, name: {hubNamedEntity2.Name}.");
					        }
					        else
					        {
						        throw new SecurityException(
							        $"The hubKey on the original entity and the updated entity have changed.  The entity was type:{hubEntity.GetType()}.");
					        }
				        }
				        
				        if (hubEntity.IsValid == false)
				        {
					        importAction = EImportAction.Delete;
				        }
				        else
				        {
					        entity.State = EntityState.Modified;
					        importAction = EImportAction.Replace;
				        }
			        }
		        }
		        		        
//		        foreach (var property in properties)
//		        {
//			        foreach (var attr in property.GetCustomAttributes(true))
//			        {
//				        // this shouldn't be possible any longer.
//				        if (attr is CopyCollectionKeyAttribute)
//				        {
//					        var value = (long) property.GetValue(item);
//					        entity.State = value <= 0 ? EntityState.Added : EntityState.Modified;
//				        }
//
//				        // if the isValid = false, then set the import action to delete.
//				        if (attr is CopyIsValidAttribute)
//				        {
//					        var value = (bool) property.GetValue(item);
//					        if (value == false)
//					        {
//						        importAction = EImportAction.Delete;
//					        }
//				        }
//			        }
//		        }

		        // add the items to the change list, with will be broadcast back to the client.
		        import.Add(item, importAction);
	        }

	        await DbContext.SaveChangesAsync(true, cancellationToken);
	        
	        if (import.Any())
	        {
		        var hub = await _cacheService.Get<DexihHub>(CacheKeys.Hub(hubKey), cancellationToken);
		        if (hub != null)
		        {
			        import.UpdateCache(hub);
		        }

		        await _cacheService.Reset(CacheKeys.HubShared(hubKey), cancellationToken);

		        await _cacheService.Update<DexihHub>(CacheKeys.Hub(hubKey), cancellationToken);

                var users = await GetHubUserIds(import.HubKey, CancellationToken.None);

                OnHubChange?.Invoke(import, users);
	        }
        }

		/// <summary>
		/// Gets a list of userIds which have access to the specified hubKey.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public Task<string[]> GetHubUserIds(long hubKey, CancellationToken cancellationToken)
		{
			return _cacheService.GetOrCreateAsync(CacheKeys.HubUserIds(hubKey), TimeSpan.FromMinutes(1), async () =>
			{
				try
				{
					var adminId = await DbContext.Roles
						.SingleAsync(c => c.Name == "ADMINISTRATOR", cancellationToken: cancellationToken);
					
					var adminUsers = await DbContext.UserRoles
						.Where(c => c.RoleId == adminId.Id)
						.Select(c => c.UserId)
						.ToArrayAsync(cancellationToken: cancellationToken);
					
					var hubUserIds = await DbContext.DexihHubUser
						.Where(c => !adminUsers.Contains(c.UserId) && c.HubKey == hubKey && c.IsValid)
						.Select(c => c.UserId).ToListAsync(cancellationToken: cancellationToken);
					
					hubUserIds.AddRange(adminUsers);

					var hubUserNames = await DbContext.Users
						.Where(c => hubUserIds.Contains(c.Id))
						.Select(c => c.Id).ToArrayAsync(cancellationToken: cancellationToken);

					return hubUserNames;
				}
				catch (Exception ex)
				{
					throw new RepositoryManagerException($"Error getting hub user ids.  {ex.Message}", ex);
				}
			}, cancellationToken);
		}

		/// <summary>
		/// Gets a list of users with names and permission to hub.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public Task<List<HubUser>> GetHubUsers(long hubKey, CancellationToken cancellationToken)
        {
            try
            {
	            var returnList = _cacheService.GetOrCreateAsync(CacheKeys.HubUsers(hubKey), TimeSpan.FromHours(1),  async () =>
	            {
		            var hubUsers = await DbContext.DexihHubUser.Where(c => c.HubKey == hubKey && c.IsValid).ToListAsync(cancellationToken: cancellationToken);
		            var users = await DbContext.Users.Where(c => hubUsers.Select(d => d.UserId).Contains(c.Id)).ToListAsync(cancellationToken: cancellationToken);

		            var hubUsersList = new List<HubUser>();
		            foreach(var hubUser in hubUsers)
		            {
			            var user = users.SingleOrDefault(c => c.Id == hubUser.UserId);
			            if (user != null)
			            {
				            hubUsersList.Add(new HubUser()
				            {
					            UserName = user.UserName,
					            FirstName = user.FirstName,
					            LastName = user.LastName,
					            Id = hubUser.UserId,
					            Permission = hubUser.Permission,
					            ReceiveAlerts = hubUser.ReceiveAlerts
				            });
			            }
		            }

		            return hubUsersList;
	            }, cancellationToken);

	            return returnList;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Error getting hub users.  {ex.Message}", ex);
            }
        }

		public async Task<string[]> GetAlertEmails(long hubKey, CancellationToken cancellationToken)
		{
			var hubUsers = await DbContext.DexihHubUser.Where(c => c.HubKey == hubKey && c.ReceiveAlerts && c.IsValid).ToListAsync(cancellationToken: cancellationToken);

			if (hubUsers.Count == 0)
			{
				return new string[0];
			}

			var emails = await DbContext.Users.Where(c => hubUsers.Select(d => d.UserId).Contains(c.Id))
				.Select(c => c.Email).ToArrayAsync(cancellationToken);

			
			return emails;
		}
        
        public async Task<DexihHub> GetUserHub(long hubKey, ApplicationUser user, CancellationToken cancellationToken)
		{
			if (user.IsAdmin)
			{
				var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.IsValid, cancellationToken: cancellationToken);
				
				if (hub == null)
				{
					throw new RepositoryManagerException($"A hub with key {hubKey} is not found.");
				}

				return hub;
			}
			else
			{
				var hubUser = await DbContext.DexihHubUser.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == user.Id && c.IsValid, cancellationToken: cancellationToken);
				
				if (hubUser == null)
				{
					throw new RepositoryManagerException($"A hub with key {hubKey} is not available to the current user.");
				}
				
				var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.IsValid, cancellationToken: cancellationToken);
				
				if (hub == null)
				{
					throw new RepositoryManagerException($"A hub with key {hubKey} is not found.");
				}
				
				return hub;
			}
		}

		public async Task<string> GetHubEncryptionKey(long hubKey, CancellationToken cancellationToken)
		{
			var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey, cancellationToken: cancellationToken);
			
			if (hub == null)
			{
				throw new RepositoryManagerException($"A hub with key {hubKey} is not found.");
			}
			
			return hub.EncryptionKey;
		}

		public async Task<DexihHub> SaveHub(DexihHub hub, ApplicationUser user, CancellationToken cancellationToken)
		{
			try
			{
				if (hub.HubKey > 0 && !user.IsAdmin)
				{
					var permission = await ValidateHubAsync(user, hub.HubKey, cancellationToken);

					if(permission != EPermission.Owner)
					{
						throw new RepositoryException("Only owners of the hub are able to make modifications.");
					}
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
					dbHub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hub.HubKey && c.IsValid, cancellationToken: cancellationToken);
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
                await DbContext.SaveChangesAsync(cancellationToken);
				await ResetHubCacheAsync(hub.HubKey, cancellationToken);
				await ResetHubPermissions(hub.HubKey, cancellationToken);

				// if new hub, then update with current user, and update the quota.
				if (isNew && !user.IsAdmin)
				{
					user.HubQuota--;
					await UpdateUserAsync(user, cancellationToken);
				}
				
				if (isNew)
				{
					await HubSetUserPermissions(dbHub.HubKey, new[] { user.Id }, EPermission.Owner, cancellationToken);
				}

				return dbHub;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save hub {hub.Name} failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihHub[]> DeleteHubs(ApplicationUser user, long[] hubKeys, CancellationToken cancellationToken)
		{
			try
			{
				var dbHubs = await DbContext.DexihHubs
					.Where(c => hubKeys.Contains(c.HubKey))
					.ToArrayAsync(cancellationToken: cancellationToken);

				foreach (var dbHub in dbHubs)
				{
					if (!user.IsAdmin)
					{
						var hubUser = await DbContext.DexihHubUser.SingleOrDefaultAsync(c => c.HubKey == dbHub.HubKey && c.UserId == user.Id && c.IsValid, cancellationToken: cancellationToken);
						if (hubUser == null || hubUser.Permission != EPermission.Owner)
						{
							throw new RepositoryManagerException($"Failed to delete the hub with name {dbHub.Name} as user does not have owner permission on this hub.");
						}
					}

					dbHub.IsValid = false;

					await ResetHubCacheAsync(dbHub.HubKey, cancellationToken);
					await ResetHubPermissions(dbHub.HubKey, cancellationToken);
				}
				
				await ResetUserCacheAsync(user.Id, cancellationToken);
				await DbContext.SaveChangesAsync(cancellationToken);
				

                return dbHubs;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete hubs failed.  {ex.Message}", ex);
            }
        }


        public async Task HubSetUserPermissions(long hubKey, IEnumerable<string> userIds, EPermission permission, CancellationToken cancellationToken)
        {
            try
            {
	            var usersHub = await DbContext.DexihHubUser.Where(c => c.HubKey == hubKey && userIds.Contains(c.UserId)).ToListAsync(cancellationToken: cancellationToken);

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

	                await ResetUserCacheAsync(userId, cancellationToken);
                    await DbContext.SaveChangesAsync(cancellationToken);
                }
	            
	            await ResetHubPermissions(hubKey, cancellationToken);

            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Hub add users failed.  {ex.Message}", ex);
            }
        }

        public async Task HubSetUserAlerts(long hubKey, IEnumerable<string> userIds, bool alertEmails, CancellationToken cancellationToken)
        {
	        try
	        {
		        var usersHub = await DbContext.DexihHubUser.Where(c => c.HubKey == hubKey && userIds.Contains(c.UserId)).ToListAsync(cancellationToken: cancellationToken);
		        foreach (var userHub in usersHub)
		        {
			        userHub.ReceiveAlerts = alertEmails;
			        await ResetUserCacheAsync(userHub.UserId, cancellationToken);
			        await DbContext.SaveChangesAsync(cancellationToken);
		        }

		        await ResetHubPermissions(hubKey, cancellationToken);
	        }
	        catch (Exception ex)
	        {
		        throw new RepositoryManagerException($"Hub set user alerts failed.  {ex.Message}", ex);
	        }
        }
        
    public async Task HubDeleteUsers(long hubKey, IEnumerable<string> userIds, CancellationToken cancellationToken)
		{
            try
            {
	            var usersHub = await DbContext.DexihHubUser.Where(c => c.HubKey == hubKey && userIds.Contains(c.UserId)).ToListAsync(cancellationToken: cancellationToken);
	            foreach (var userHub in usersHub)
	            {
		            userHub.IsValid = false;
		            await ResetUserCacheAsync(userHub.UserId, cancellationToken);
	            }
	            await DbContext.SaveChangesAsync(cancellationToken);
	            await ResetHubPermissions(hubKey, cancellationToken);
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Hub delete users failed.  {ex.Message}", ex);
            }

        }
        #endregion

        #region Encrypt Functions
        public async Task<string> DecryptStringAsync(long hubKey, string value, CancellationToken cancellationToken)
		{
			var key = await GetHubEncryptionKey(hubKey, cancellationToken);
			var decryptResult = Dexih.Utils.Crypto.EncryptString.Decrypt(value, key, 1000);
			return decryptResult;
		}

		public async Task<string> EncryptStringAsync(long hubKey, string value, CancellationToken cancellationToken)
		{
			var key = await GetHubEncryptionKey(hubKey, cancellationToken);
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
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		/// <exception cref="ApplicationUserException"></exception>
		public Task<EPermission> ValidateHubAsync(ApplicationUser user, long hubKey, CancellationToken cancellationToken)
		{
			var validate = _cacheService.GetOrCreateAsync(CacheKeys.UserHubPermission(user.Id, hubKey), TimeSpan.FromMinutes(1),  async () =>
			{
				if (!user.EmailConfirmed)
				{
					throw new ApplicationUserException("The users email address has not been confirmed.");
				}

				var hub = await DbContext.DexihHubs.SingleOrDefaultAsync(c => c.IsValid && c.HubKey == hubKey, cancellationToken: cancellationToken);

				if (hub == null)
				{
					throw new ApplicationUserException("The hub with the key: " + hubKey + " could not be found.");
				}

				if (user.IsAdmin)
				{
					return EPermission.Owner;
				}

				var hubUser =
					await DbContext.DexihHubUser.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == user.Id && c.IsValid, cancellationToken: cancellationToken);

				if (hubUser.Permission == EPermission.Suspended ||
				    hubUser.Permission == EPermission.None)
				{
					throw new ApplicationUserException($"The users does not have access to the hub with key {hubKey}.");
				}
				else
				{
					return hubUser.Permission;
				}
			}, cancellationToken);

			return validate;
		}
		
		#endregion
		
        #region Connection Functions
        public async Task<DexihConnection> SaveConnection(long hubKey, DexihConnection connection, CancellationToken cancellationToken)
		{
			try
			{
				DexihConnection dbConnection;

				//check there are no connections with the same name
				var sameName = await DbContext.DexihConnections.FirstOrDefaultAsync(c => 
                        c.HubKey == hubKey &&
                        c.Name == connection.Name && 
                        c.Key != connection.Key && 
                        c.IsValid, cancellationToken: cancellationToken);

				if (sameName != null)
				{
                    throw new RepositoryManagerException($"The name \"{connection.Name}\" already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (connection.Key > 0)
				{
					dbConnection = await DbContext.DexihConnections.SingleOrDefaultAsync(d => d.IsValid && d.Key == connection.Key, cancellationToken: cancellationToken);
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

//				if (!string.IsNullOrEmpty(dbConnection.PasswordRaw))
//				{
//					// if the UsePasswordVariable variable is set, then do not encrypt the password (which will be a variable name).
//					dbConnection.Password = dbConnection.UsePasswordVariable ? dbConnection.PasswordRaw : await EncryptString(connection.HubKey, dbConnection.PasswordRaw, cancellationToken);
//				}
//
//				if (!string.IsNullOrEmpty(dbConnection.ConnectionStringRaw))
//				{
//					// if the UseConnectionStringVariable is set, then do not encrypt the password (which will be a variable name).
//					dbConnection.ConnectionString = dbConnection.UseConnectionStringVariable ? dbConnection.ConnectionStringRaw : await EncryptString(connection.HubKey, dbConnection.ConnectionStringRaw, cancellationToken);
//				}

				dbConnection.HubKey = hubKey;
				dbConnection.UpdateDate = DateTime.UtcNow;
				dbConnection.IsValid = true;

				await SaveHubChangesAsync(hubKey, cancellationToken);
				
				// var dbConnection2 = await DbContext.DexihConnections.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.ConnectionKey == dbConnection.ConnectionKey);

				return dbConnection;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save connection \"{connection.Name}\" failed.  {ex.Message}", ex);
            }
        }

		public async Task<DexihConnection> DeleteConnection(long hubKey, long connectionKey, CancellationToken cancellationToken)
		{
			var connections = await DeleteConnections(hubKey, new[] {connectionKey}, cancellationToken);
			return connections[0];
		}

        public async Task<DexihConnection[]> DeleteConnections(long hubKey, long[] connectionKeys, CancellationToken cancellationToken)
		{
            try
            {
                var dbConnections = await DbContext.DexihConnections
                    .Where(c => c.HubKey == hubKey && connectionKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var connection in dbConnections)
                {
	                connection.IsValid = false;
                }

                var dbtables = await DbContext.DexihTables
	                .Where(c => connectionKeys.Contains(c.ConnectionKey) && c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);
                
                foreach (var table in dbtables)
                {
	                table.IsValid = false;

	                foreach (var column in table.DexihTableColumns)
	                {
		                column.IsValid = false;
	                }
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbConnections;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete connections failed.  {ex.Message}", ex);
            }
        }



        public async Task<DexihConnection> GetConnection(long hubKey, long connectionKey, bool includeTables, CancellationToken cancellationToken)
		{
            try
            {
                var dbConnection = await DbContext.DexihConnections
                      .SingleOrDefaultAsync(c => c.Key == connectionKey && c.HubKey == hubKey && c.IsValid, cancellationToken: cancellationToken);

                if (dbConnection == null)
                {
                    throw new RepositoryManagerException($"The connection with the key {connectionKey} could not be found.");
                }

                if (includeTables)
                {
                    var cache = new CacheManager(dbConnection.HubKey, "", _logger);
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

		public async Task<DexihTable> SaveTable(long hubKey,DexihTable hubTable, bool includeColumns, bool includeFileFormat, CancellationToken cancellationToken)
		{
			var tables = await SaveTables(hubKey, new[] {hubTable}, includeColumns, includeFileFormat, cancellationToken);
			return tables[0];
		}
		
		public async Task<DexihTable[]> SaveTables(long hubKey, IEnumerable<DexihTable> tables, bool includeColumns, bool includeFileFormat, CancellationToken cancellationToken)
		{
			try
			{
			
				var savedTables = new List<DexihTable>();
				foreach (var table in tables)
				{
					DexihTable dbTable;

					//check there are no datajobs with the same name
					var sameName = await DbContext.DexihTables.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.ConnectionKey == table.ConnectionKey && c.Name == table.Name && c.Key != table.Key && c.IsValid, cancellationToken: cancellationToken);
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
						var dbFileFormat = await DbContext.DexihFileFormats.SingleOrDefaultAsync(f => f.IsValid && f.HubKey == hubKey && f.Key == table.FileFormat.Key, cancellationToken: cancellationToken);
						if (dbFileFormat == null)
						{
							table.EntityStatus.Message = $"The table could not be saved as the table contains the fileformat {table.FileFormat.Key} that no longer exists in the repository.";
							table.EntityStatus.LastStatus = EStatus.Error;
                            throw new RepositoryManagerException(table.EntityStatus.Message);
                        }

                        table.FileFormat.CopyProperties(dbFileFormat, true);
						table.FileFormat = dbFileFormat;
					}

					var dbConnection = await DbContext.DexihConnections.SingleOrDefaultAsync(c => c.IsValid && c.HubKey == hubKey && c.Key == table.ConnectionKey, cancellationToken: cancellationToken);
                    if (dbConnection == null)
                    {
                        table.EntityStatus.Message = $"The table could not be saved as the table contains connection that no longer exists in the repository.";
                        table.EntityStatus.LastStatus = EStatus.Error;
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
						table.ResetKeys();
						table.CopyProperties(dbTable, false);
						DbContext.DexihTables.Add(dbTable);
						savedTables.Add(dbTable);
					}
					else
					{
						dbTable = await GetTable(hubKey, table.Key, true, cancellationToken);
						
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
							dbTable.UpdateDate = DateTime.UtcNow; // change update date to force table to become modified entity.
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

					dbTable.UpdateDate = DateTime.UtcNow;
					dbTable.IsValid = true;
					dbTable.HubKey = hubKey;
					
					_logger.LogTrace("Saving table: " + table.Name);
				}
				
				await SaveHubChangesAsync(hubKey, cancellationToken);				
				return savedTables.ToArray();
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save tables failed.  {ex.Message}", ex);
            }
        }

		public async Task<DexihTable> DeleteTable(long hubKey, long tableKey, CancellationToken cancellationToken)
		{
			var tables = await DeleteTables(hubKey, new[] {tableKey}, cancellationToken);
			return tables[0];
		}


        public async Task<DexihTable[]> DeleteTables(long hubKey, long[] tableKeys, CancellationToken cancellationToken)
		{
            try
            {
                var dbTables = await DbContext.DexihTables
                    .Include(d => d.DexihTableColumns)
                    .Include(f => f.FileFormat)
                    .Where(c => c.HubKey == hubKey && tableKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

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

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbTables;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete tables failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihTable[]> ShareTables(long hubKey, long[] tableKeys, bool isShared, CancellationToken cancellationToken)
        {
            try
            {
                var dbTables = await DbContext.DexihTables
                    .Include(d => d.DexihTableColumns)
                    .Include(f => f.FileFormat)
                    .Where(c => c.HubKey == hubKey && tableKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var table in dbTables)
                {
                    table.IsShared = isShared;
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbTables;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Share tables failed.  {ex.Message}", ex);
            }

        }
		
		public async Task<DexihTable[]> GetTables(long hubKey, IEnumerable<long> tableKeys, bool includeColumns, CancellationToken cancellationToken)
		{
			try
			{
				var dbTables = await DbContext.DexihTables.Where(c => tableKeys.Contains(c.Key) && c.HubKey == hubKey && c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);

				if (includeColumns)
				{
					await DbContext.DexihTableColumns
						.Where(c => c.TableKey != null && c.IsValid && c.HubKey == hubKey && tableKeys.Contains(c.TableKey.Value))
						.Include(c=>c.ChildColumns)
						.LoadAsync(cancellationToken: cancellationToken);
				}

				return dbTables;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get table with keys {string.Join(",", tableKeys)} failed.  {ex.Message}", ex);
			}
		}
		

        public async Task<DexihTable> GetTable(long hubKey, long tableKey, bool includeColumns, CancellationToken cancellationToken)
		{
            try
            {
                var dbTable = await DbContext.DexihTables
	                .SingleOrDefaultAsync(c => c.Key == tableKey && c.HubKey == hubKey && c.IsValid, cancellationToken: cancellationToken);

                if (dbTable == null)
                {
                    throw new RepositoryManagerException($"The table with the key {tableKey} could not be found.");
                }

	            if (dbTable.FileFormatKey != null)
	            {
		            dbTable.FileFormat =
			            await DbContext.DexihFileFormats.SingleOrDefaultAsync(
				            c => c.Key == dbTable.FileFormatKey && c.IsValid, cancellationToken: cancellationToken);
	            }

                if (includeColumns)
                {
                    await DbContext.Entry(dbTable).Collection(a => a.DexihTableColumns).Query()
						.Where(c => c.HubKey == hubKey && c.IsValid && dbTable.Key == c.TableKey)
						.Include(c=>c.ChildColumns).LoadAsync(cancellationToken: cancellationToken);
                }

                return dbTable;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get table with key {tableKey} failed.  {ex.Message}", ex);
            }
        }

		public async Task<DexihView> GetView(long hubKey, long viewKey, CancellationToken cancellationToken)
		{
			try
			{
				var dbView = await DbContext.DexihViews
					.SingleOrDefaultAsync(c => c.Key == viewKey && c.HubKey == hubKey && c.IsValid, cancellationToken: cancellationToken);

				if (dbView == null)
				{
					throw new RepositoryManagerException($"The view with the key {viewKey} could not be found.");
				}

				await DbContext.Entry(dbView).Collection(a => a.Parameters).Query()
					.Where(c => c.HubKey == hubKey && c.IsValid && dbView.Key == c.ViewKey).LoadAsync(cancellationToken: cancellationToken);

				return dbView;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get view with key {viewKey} failed.  {ex.Message}", ex);
			}
		}

		public async Task<DexihDashboard> GetDashboard(long hubKey, long dashboardKey, CancellationToken cancellationToken)
		{
			try
			{
				var dbDashboard = await DbContext.DexihDashboards
					.SingleOrDefaultAsync(c => c.Key == dashboardKey && c.HubKey == hubKey && c.IsValid, cancellationToken: cancellationToken);

				if (dbDashboard == null)
				{
					throw new RepositoryManagerException($"The dashboard with the key {dashboardKey} could not be found.");
				}

				await DbContext.Entry(dbDashboard).Collection(a => a.Parameters).Query()
					.Where(c => c.HubKey == hubKey && c.IsValid && dbDashboard.Key == c.DashboardKey).LoadAsync(cancellationToken: cancellationToken);

				var items = await DbContext.DexihDashboardItems.Where(c => c.HubKey == hubKey && c.IsValid && dbDashboard.Key == c.DashboardKey).ToHashSetAsync(cancellationToken: cancellationToken);
				await DbContext.DexihDashboardItemParameters.Where(c => c.IsValid && c.HubKey == hubKey && items.Select(k => k.Key).Contains(c.DashboardItemKey))
					.LoadAsync(cancellationToken: cancellationToken);
			
				return dbDashboard;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get dashboard with key {dashboardKey} failed.  {ex.Message}", ex);
			}
		}

		/// <summary>
		/// Gets the view attached to a dashboard item.
		/// </summary>
		/// <param name="hubKey"></param>
		/// <param name="dashboardItemKey"></param>
		/// <param name="isShared">Returns error unless the dashboard is shared.</param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		/// <exception cref="RepositoryManagerException"></exception>
		public async Task<DexihView> GetDashboardItemView(long hubKey, long dashboardItemKey, bool isShared, CancellationToken cancellationToken)
		{
			try
			{
				var dashboardIem = await DbContext.DexihDashboardItems.SingleOrDefaultAsync(
					c => c.HubKey == hubKey && c.Key == dashboardItemKey && c.IsValid, cancellationToken: cancellationToken);

				if (dashboardIem == null)
				{
					throw new RepositoryManagerException($"The dashboard item with key {dashboardItemKey} does not exist."); 	
				}
				
				if (isShared)
				{
					var dashboard = await DbContext.DexihDashboards.SingleOrDefaultAsync(c =>
						c.HubKey == hubKey && c.Key == dashboardIem.DashboardKey && c.IsValid, cancellationToken: cancellationToken);

					if (dashboard == null)
					{
						throw new RepositoryManagerException($"The dashboard with the key {dashboardIem.Key} was not found.");
					}

					if (!dashboard.IsShared)
					{
						throw new RepositoryManagerException(
							$"The dashboard {dashboard.Name} is not shared.");
					}
				}

				var view = await DbContext.DexihViews.SingleOrDefaultAsync(c => c.HubKey == hubKey && c.Key == dashboardIem.ViewKey && c.IsValid, cancellationToken: cancellationToken);

				if (view == null)
				{
					throw new RepositoryManagerException($"The dashboard item {dashboardIem.Name} has a view with the key {dashboardIem.ViewKey} which was not found.");
				}

				return view;

			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get dashboard item with key {dashboardItemKey} failed.  {ex.Message}", ex);
			}
		}
		public async Task<DexihListOfValues> GetListOfValues(long hubKey, long listOfValuesKey, CancellationToken cancellationToken)
		{
			try
			{
				var dbListOfValues = await DbContext.DexihListOfValues
					.SingleOrDefaultAsync(c => c.Key == listOfValuesKey && c.HubKey == hubKey && c.IsValid, cancellationToken: cancellationToken);

				if (dbListOfValues == null)
				{
					throw new RepositoryManagerException($"The list of values with the key {listOfValuesKey} could not be found.");
				}

				return dbListOfValues;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Get list of values with key {listOfValuesKey} failed.  {ex.Message}", ex);
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
        public EUpdateStrategy GetBestUpdateStrategy(DexihTable hubTable)
		{
            try
            {
                //TODO Improve get best strategy 

	            if (hubTable == null)
		            return EUpdateStrategy.Reload;
	            else if (hubTable.DexihTableColumns.Count(c => c.DeltaType == EDeltaType.NaturalKey) == 0)
	            {
		            // no natural key.  Reload is the only choice
		            return EUpdateStrategy.Reload;
	            }
	            else
	            {
		            if (hubTable.IsVersioned)
			            return EUpdateStrategy.AppendUpdateDeletePreserve;
		            else
			            return EUpdateStrategy.AppendUpdateDelete;
	            }
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Get best update strategy failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihDatalink[]> SaveDatalinks(long hubKey, DexihDatalink[] hubDatalinks, bool includeTargetTable, CancellationToken cancellationToken)
		{
			try
			{
				var savedDatalinks = new List<DexihDatalink>();
				foreach (var datalink in hubDatalinks)
				{
					//check there are no datajobs with the same name
					var sameName = await DbContext.DexihDatalinks.FirstOrDefaultAsync(c =>
						c.HubKey == hubKey && c.Name == datalink.Name && c.Key != datalink.Key &&
						c.IsValid, cancellationToken: cancellationToken);
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
						var cacheManager = new CacheManager(hubKey, "", _logger);
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

					existingDatalink.UpdateDate = DateTime.UtcNow;
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
//	                    var cacheManager = new CacheManager(hubKey, "", _logger);
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
//	                    existingDatalink.UpdateDate = DateTime.UtcNow;
//                        savedDatalinks.Add(existingDatalink);
//                    }

					// uncomment to check changes.
//					var modifiedEntries = DbContext.ChangeTracker
//						.Entries()
//						.Where(x => x.State == EntityState.Modified || x.State == EntityState.Added || x.State == EntityState.Deleted)
//						.Select(x => x)
//						.ToList();
					
					await SaveHubChangesAsync(hubKey, cancellationToken);

                }

  

                //await DbContext.DexihUpdateStrategies.LoadAsync();
                return savedDatalinks.ToArray();
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save datalinks failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihDatalink[]> DeleteDatalinks(long hubKey, long[] datalinkKeys, CancellationToken cancellationToken)
		{
			try
			{
				var cache = new CacheManager(hubKey, "", _logger);
				var dbDatalinks = await cache.GetDatalinksAsync(datalinkKeys, DbContext);

				foreach (var dbDatalink in dbDatalinks)
				{
					dbDatalink.IsValid = false;
					foreach (var transform in dbDatalink.DexihDatalinkTransforms)
					{
						transform.IsValid = false;
						foreach (var item in transform.DexihDatalinkTransformItems)
						{
							item.IsValid = false;
							foreach (var funcParam in item.DexihFunctionParameters)
							{
								funcParam.IsValid = false;

								foreach (var arrParam in funcParam.ArrayParameters)
								{
									arrParam.IsValid = false;
								}
							}
						}
					}

					foreach (var profile in dbDatalink.DexihDatalinkProfiles)
					{
						profile.IsValid = false;
					}

					foreach (var step in dbDatalink.DexihDatalinkSteps)
					{
						step.IsValid = false;
						foreach (var dep in step.DexihDatalinkDependencies)
						{
							dep.IsValid = false;
						}
					}
				}

				await SaveHubChangesAsync(hubKey, cancellationToken);

				return dbDatalinks;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete datalinks failed.  {ex.Message}", ex);
            }
        }

        public async Task ShareItems(long hubKey, long[] keys, EDataObjectType objectType, bool isShared, CancellationToken cancellationToken)
        {
            try
            {
	            var cache = new CacheManager(hubKey, "", _logger);
	            
	            switch (objectType)
	            {
		            case EDataObjectType.Table:
			            foreach (var table in await cache.GetTablesAsync(keys, DbContext))
			            {
				            table.IsShared = isShared;
			            }
			            break;
		            case EDataObjectType.Datalink:
			            foreach (var datalink in await cache.GetDatalinksAsync(keys, DbContext))
			            {
				            datalink.IsShared = isShared;
			            }
			            break;
		            case EDataObjectType.View:
			            foreach (var view in await cache.GetViewsAsync(keys, DbContext))
			            {
				            view.IsShared = isShared;
			            }
			            break;
		            case EDataObjectType.Dashboard:
			            foreach (var dashboard in await cache.GetDashboardsAsync(keys, DbContext))
			            {
				            dashboard.IsShared = isShared;
			            }
			            break;
		            case EDataObjectType.Api:
			            foreach (var api in await cache.GetApisAsync(keys, DbContext))
			            {
				            api.IsShared = isShared;
			            }
			            break;
		            default:
			            throw new ArgumentOutOfRangeException(nameof(objectType), objectType, null);
	            }

                await SaveHubChangesAsync(hubKey, cancellationToken);
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Share datalinks failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihDatalink[]> NewDatalinks(long hubKey, 
	        string datalinkName, 
	        EDatalinkType datalinkType, 
	        long? targetConnectionKey, 
	        long[] sourceTableKeys, 
	        long? targetTableKey, 
	        string targetTableName, 
	        long? auditConnectionKey, 
	        bool addSourceColumns,
	        EDeltaType[] auditColumns,
	        NamingStandards namingStandards, CancellationToken cancellationToken)
		{
			try
			{
				if (namingStandards == null)
				{
					namingStandards = new NamingStandards();
					namingStandards.LoadDefault();
				}

				var newDatalinks = new List<DexihDatalink>();

				if (sourceTableKeys.Length == 0)
				{
					sourceTableKeys = new long[] { 0 };
				}

                long tempColumnKeys = -1;

				var sourceTables = await DbContext.DexihTables.Where(c => c.HubKey == hubKey && sourceTableKeys.Contains(c.Key) && c.IsValid).ToDictionaryAsync(c => c.Key, cancellationToken: cancellationToken);
				await DbContext.DexihTableColumns.Where(c=> c.HubKey == hubKey && c.TableKey != null && sourceTables.Keys.Contains(c.TableKey.Value) && c.IsValid).Include(c => c.ChildColumns).LoadAsync(cancellationToken: cancellationToken);
				var targetTable = targetTableKey == null ? null : await DbContext.DexihTables.SingleOrDefaultAsync(c => c.IsValid && c.HubKey == hubKey && c.Key == targetTableKey, cancellationToken: cancellationToken);
				var targetCon = targetConnectionKey == null ? null : await DbContext.DexihConnections.SingleOrDefaultAsync(c => c.IsValid && c.HubKey == hubKey && c.Key == targetConnectionKey, cancellationToken: cancellationToken);

				foreach (var sourceTableKey in sourceTableKeys)
				{
					if (sourceTableKey == 0 && targetTableKey != null && targetTableName != null)
					{
                        throw new RepositoryManagerException("There is no source table selected, so the target table cannot be auto-named and must be defined.");
					}

					if (!string.IsNullOrEmpty(datalinkName) && await DbContext.DexihDatalinks.AnyAsync(c => c.Name == datalinkName && c.IsValid, cancellationToken: cancellationToken))
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
						while (await DbContext.DexihDatalinks.AnyAsync(c => c.HubKey == hubKey && c.Name == newName[0] && c.IsValid, cancellationToken: cancellationToken))
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
					foreach (var column in sourceTable.DexihTableColumns.OrderBy(c => c.Position).Where(c => c.IsValid))
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
		                    targetTable = await CreateDefaultTargetTable(hubKey, datalinkType, sourceTable, targetTableName, targetCon, addSourceColumns, auditColumns, namingStandards, cancellationToken);
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
                                var item = new DexihDatalinkTransformItem
                                {
                                    Position = position++,
                                    TransformItemType = ETransformItemType.ColumnPair,
                                    SourceDatalinkColumn = sourceColumn
                                };


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

				var savedDatalinks = await SaveDatalinks(hubKey, newDatalinks.ToArray(), true, cancellationToken);
				return savedDatalinks;

			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Create new datalinks failed.  {ex.Message}", ex);
            }

        }
        #endregion

        #region Datajob Functions
        
        public async Task<DexihDatajob[]> SaveDatajobs(long hubKey, DexihDatajob[] hubDatajobs, CancellationToken cancellationToken)
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
					var sameName = await DbContext.DexihDatajobs.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == datajob.Name && c.Key != datajob.Key && c.IsValid, cancellationToken: cancellationToken);
					if (sameName != null)
					{
                        throw new RepositoryManagerException($"A datajob with the name {datajob.Name} already exists in the repository.");
					}

					foreach (var trigger in datajob.DexihTriggers)
					{
						if (trigger.Key < 0)
						{
							trigger.Key = 0;
						}
					}

                    foreach(var step in datajob.DexihDatalinkSteps)
                    {
                        foreach(var dep in step.DexihDatalinkDependencies)
                        {
                            //if (dep.Key < 0)
                            //{
                            //    dep.Key = 0;
                            //}
                            
                            if (dep.DependentDatalinkStepKey <= 0)
                            {
                                dep.DependentDatalinkStep = datajob.DexihDatalinkSteps.SingleOrDefault(c => c.Key == dep.DependentDatalinkStepKey);
                                dep.DatalinkStepKey = 0;
                            }
                        }
                    }



                    if (datajob.Key <= 0) {
						datajob.Key = 0;
						// var newDatajob = new DexihDatajob();
						// datajob.CopyProperties(newDatajob, false);
						datajob.ResetKeys();
						DbContext.DexihDatajobs.Add(datajob);
						savedDatajobs.Add(datajob);
					}
					else
					{
						var cacheManager = new CacheManager(hubKey, "", _logger);
						var originalDatajob = await cacheManager.GetDatajob(datajob.Key, DbContext);

						if(originalDatajob == null)
						{
							datajob.Key = 0;
							var newDatajob = new DexihDatajob();
							datajob.ResetKeys();
							datajob.CopyProperties(newDatajob, false);
							DbContext.DexihDatajobs.Add(newDatajob);
							savedDatajobs.Add(datajob);
						}
						else 
						{
							datajob.CopyProperties(originalDatajob);
							
							foreach (var step in datajob.DexihDatalinkSteps.Where(c => c.Key < 0))
							{
							    step.ResetKeys();
							}
							
							originalDatajob.UpdateDate = DateTime.UtcNow;
							savedDatajobs.Add(originalDatajob);
						}
					}
				}

				await SaveHubChangesAsync(hubKey, cancellationToken);
				return savedDatajobs.ToArray();
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save datajobs failed.  {ex.Message}", ex);
            }
        }

        public async Task<bool> DeleteDatajobs(long hubKey, long[] datajobKeys, CancellationToken cancellationToken)
		{
			try
			{
				var datajobs = await DbContext.DexihDatajobs
					.Include(d => d.DexihTriggers)
					.Include(s => s.DexihDatalinkSteps)
						.ThenInclude(d => d.DexihDatalinkDependencies)
					.Where(c => c.HubKey == hubKey && datajobKeys.Contains(c.Key))
					.ToArrayAsync(cancellationToken: cancellationToken);

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

				await SaveHubChangesAsync(hubKey, cancellationToken);
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
        /// <param name="iPAddress"></param>
        /// <param name="remoteAgentId"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<DexihRemoteAgent> RemoteAgentLogin(string iPAddress, string remoteAgentId, CancellationToken cancellationToken)
		{
            try
            {
	            var remoteAgent = await DbContext.DexihRemoteAgents.SingleOrDefaultAsync(c => 
		            c.RemoteAgentId == remoteAgentId && 
		            c.IsValid, cancellationToken: cancellationToken);

	            if (remoteAgent == null)
	            {
		            return null;
	            }

	            remoteAgent.LastLoginDateTime = DateTime.UtcNow;
				remoteAgent.LastLoginIpAddress = iPAddress;
				await DbContext.SaveChangesAsync(cancellationToken);
				return remoteAgent;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Remote agent login failed.  {ex.Message}", ex);
            }
        }
		
		public Task<DexihRemoteAgent> GetRemoteAgent(long remoteAgentKey, CancellationToken cancellationToken)
		{
			return DbContext.DexihRemoteAgents.SingleOrDefaultAsync(
				c => c.RemoteAgentKey == remoteAgentKey && c.IsValid, cancellationToken: cancellationToken);
		}
		
	   public async Task<DexihRemoteAgent> SaveRemoteAgent(string userId, DexihRemoteAgent hubRemoteAgent, CancellationToken cancellationToken)
		{
            try
            {
	            DexihRemoteAgent dbRemoteAgent;
	            
                //if there is a remoteAgentKey, retrieve the record from the database, and copy the properties across.
                if (hubRemoteAgent.RemoteAgentKey > 0)
                {
                    dbRemoteAgent = await DbContext.DexihRemoteAgents.SingleOrDefaultAsync(d => d.RemoteAgentKey == hubRemoteAgent.RemoteAgentKey && d.IsValid, cancellationToken: cancellationToken);
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

                dbRemoteAgent.UpdateDate = DateTime.UtcNow;
                dbRemoteAgent.IsValid = true;

	            await DbContext.SaveChangesAsync(cancellationToken);

                return dbRemoteAgent;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save remote agents failed.  {ex.Message}", ex);
            }
        }
		
		public async Task<bool> DeleteRemoteAgent(ApplicationUser user, long remoteAgentKey, CancellationToken cancellationToken)
		{
			try
			{
				var dbItem = await DbContext.DexihRemoteAgents
					.SingleAsync(c => c.RemoteAgentKey == remoteAgentKey, cancellationToken: cancellationToken);

				if (dbItem.UserId != user.Id && !user.IsAdmin)
				{
					throw new RepositoryManagerException($"The remote agent {dbItem.Name} could not be removed as the current user {user.Email} did not create the remote agent.");
				}

				dbItem.IsValid = false;

				// remove any permissions for the hub.
				var hubs = DbContext.DexihRemoteAgentHubs.Where(c => c.RemoteAgentKey == dbItem.RemoteAgentKey);
				foreach(var hub in hubs)
				{
					hub.IsValid = false;
				}
				
				await DbContext.SaveChangesAsync(cancellationToken);
				
				return true;
			}
			catch (Exception ex)
			{
				throw new RepositoryManagerException($"Delete remote agents failed.  {ex.Message}", ex);
			}
		}

		///// <summary>
		///// Gets a list of all hubs which have been authorized for the remote agent.
		///// </summary>
		///// <param name="remoteSettings"></param>
		///// <returns></returns>
		//public async Task<DexihRemoteAgentHub[]> AuthorizedRemoteAgentHubs(RemoteSettings remoteSettings, CancellationToken cancellationToken)
		//{
		//	return await _cacheService.GetOrCreateAsync(CacheKeys.RemoteAgentHubs(remoteSettings.AppSettings.RemoteAgentId), TimeSpan.FromMinutes(1), 
		//		async () =>
		//		{
		//			var hubs = await GetUserHubs(remoteSettings.Runtime.User, cancellationToken);
			
		//			var remoteAgents = await DbContext.DexihRemoteAgentHubs.Where(c => 
		//				c.RemoteAgent.RemoteAgentId == remoteSettings.AppSettings.RemoteAgentId &&
		//				hubs.Select(h => h.HubKey).Contains(c.HubKey) &&
		//				c.IsAuthorized &&
		//				c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);

		//			return remoteAgents;
		//		}, cancellationToken);
		//}

		/// <summary>
		/// Gets a list of all remote agents available to the user.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public async Task<DexihRemoteAgentHub[]> AuthorizedUserRemoteAgentHubs(ApplicationUser user, CancellationToken cancellationToken)
		{
			return await _cacheService.GetOrCreateAsync( CacheKeys.RemoteAgentUserHubs(user.Id), TimeSpan.FromMinutes(1), 
				async () =>
				{
					var hubs = await GetUserHubs(user, cancellationToken);
			
					var remoteAgents = await DbContext.DexihRemoteAgentHubs.Include(c => c.RemoteAgent).Where(c => 
						hubs.Select(h => h.HubKey).Contains(c.HubKey) &&
						c.IsAuthorized &&
						c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);

					return remoteAgents;
				}, cancellationToken);
		}
		
		/// <summary>
		/// Gets a list of all hubs which have been authorized for the remote agent.
		/// </summary>
		/// <returns></returns>
		public async Task<DexihRemoteAgentHub[]> AuthorizedRemoteAgentHubs(long remoteAgentKey, CancellationToken cancellationToken)
		{
			return await _cacheService.GetOrCreateAsync(CacheKeys.RemoteAgentKeyHubs(remoteAgentKey),TimeSpan.FromMinutes(1), 
				async () =>
				{
					var remoteAgents = await DbContext.DexihRemoteAgentHubs.Where(c => 
						c.RemoteAgentKey == remoteAgentKey &&
						c.IsAuthorized &&
						c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);

					return remoteAgents;
				}, cancellationToken);
		}

		public Task<DexihRemoteAgentHub> AuthorizedRemoteAgentHub(long hubKey, long remoteAgentKey, CancellationToken cancellationToken)
		{
			return DbContext.DexihRemoteAgentHubs.FirstOrDefaultAsync(d => d.HubKey == hubKey && d.RemoteAgentKey == remoteAgentKey && d.IsAuthorized && d.IsValid, cancellationToken: cancellationToken);
		}

		public async Task<DexihRemoteAgent[]> GetRemoteAgents(ApplicationUser user, CancellationToken cancellationToken)
		{
			var userHubs = (await GetUserHubs(user, cancellationToken)).Select(c=>c.HubKey);
			
			var remoteAgents = await DbContext.DexihRemoteAgents.Where(c => 
				c.IsValid && 
				(user.IsAdmin || c.UserId == user.Id || c.DexihRemoteAgentHubs.Any(d => userHubs.Contains(d.HubKey)))
				).ToArrayAsync(cancellationToken: cancellationToken);
			return remoteAgents;
		}

        public async Task<DexihRemoteAgentHub> SaveRemoteAgentHub(string userId, long hubKey, DexihRemoteAgentHub hubRemoteAgent, CancellationToken cancellationToken)
		{
            try
            {
	            DexihRemoteAgentHub dbRemoteAgent;
	            
                //if there is a remoteAgentKey, retrieve the record from the database, and copy the properties across.
                if (hubRemoteAgent.RemoteAgentHubKey > 0)
                {
                    dbRemoteAgent = await DbContext.DexihRemoteAgentHubs.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.RemoteAgentHubKey == hubRemoteAgent.RemoteAgentHubKey && d.IsValid, cancellationToken: cancellationToken);
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

                dbRemoteAgent.UpdateDate = DateTime.UtcNow;
                dbRemoteAgent.IsValid = true;

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbRemoteAgent;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save remote agents failed.  {ex.Message}", ex);
            }

        }

        public async Task<bool> DeleteRemoteAgentHub(long hubKey, long remoteAgentHubKey, CancellationToken cancellationToken)
		{
            try
            {
                var dbItem = await DbContext.DexihRemoteAgentHubs
                    .SingleAsync(c => c.HubKey == hubKey && c.RemoteAgentHubKey == remoteAgentHubKey, cancellationToken: cancellationToken);

                dbItem.IsValid = false;
                await SaveHubChangesAsync(hubKey, cancellationToken);

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
                    TransformType = transform.TransformType,
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
	        EDatalinkType datalinkType, 
	        DexihTable sourceTable, 
	        string tableName, 
	        DexihConnection targetConnection,
	        bool addSourceColumns,
	        EDeltaType[] auditColumns,
	        NamingStandards namingStandards, CancellationToken cancellationToken)
		{
            try
            {
                DexihTable hubTable;

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

					if(await DbContext.DexihTables.AnyAsync(c => c.HubKey == hubKey && c.ConnectionKey == targetConnection.Key && c.Name == tableName && c.IsValid, cancellationToken: cancellationToken))
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
					while (await DbContext.DexihTables.AnyAsync(c => c.HubKey == hubKey && c.ConnectionKey == targetConnection.Key && c.Name == newName && c.IsValid, cancellationToken: cancellationToken))
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
	                if (sourceTable.DexihTableColumns.Count(c => c.DeltaType == EDeltaType.AutoIncrement) > 0)
	                {
		                hubTable.DexihTableColumns.Add(NewDefaultTableColumn("SourceSurrogateKey", namingStandards, hubTable.Name, ETypeCode.Int64, EDeltaType.SourceSurrogateKey, position++));
	                }
                }

	            // add the default for each of the requested auditColumns.
	            if (auditColumns != null)
	            {
		            foreach (var auditColumn in auditColumns)
		            {
			            var exists =
				            hubTable.DexihTableColumns.FirstOrDefault(c => c.DeltaType == auditColumn && c.IsValid);
			            if (exists == null)
			            {
				            var dataType = GetDeltaDataType(auditColumn);
				            var newColumn =
					            NewDefaultTableColumn(auditColumn.ToString(), namingStandards, hubTable.Name, dataType,
						            auditColumn, position++);

				            // ensure the name is unique.
				            string[] baseName = {newColumn.Name};
				            var version = 1;
				            while (hubTable.DexihTableColumns.FirstOrDefault(c => c.Name == baseName[0] && c.IsValid) !=
				                   null)
				            {
					            baseName[0] = newColumn.Name + (version++);
				            }

				            newColumn.Name = baseName[0];

				            hubTable.DexihTableColumns.Add(newColumn);
			            }
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

        public async Task<DexihColumnValidation[]> DeleteColumnValidations(long hubKey, long[] columnValidationKeys, CancellationToken cancellationToken)
		{
            try
            {
                var dbValidations = await DbContext.DexihColumnValidations
                    .Include(column => column.DexihColumnValidationColumn)
                    .Where(c => c.HubKey == hubKey && columnValidationKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var validation in dbValidations)
                {
                    validation.IsValid = false;
                    foreach (var column in validation.DexihColumnValidationColumn)
                    {
                        column.ColumnValidationKey = null;
                    }
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbValidations;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete column validations failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihColumnValidation> SaveColumnValidation(long hubKey, DexihColumnValidation validation, CancellationToken cancellationToken)
		{
			try
			{
				DexihColumnValidation dbColumnValidation;

				//check there are no connections with the same name
				var sameName = await DbContext.DexihColumnValidations.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == validation.Name && c.Key != validation.Key && c.IsValid, cancellationToken: cancellationToken);
				if (sameName != null)
				{
                    throw new RepositoryManagerException($"A column validation with the name {validation.Name} already exists in the repository.");
				}

				//if there is a connectionKey, retrieve the record from the database, and copy the properties across.
				if (validation.Key > 0)
				{
					dbColumnValidation = await DbContext.DexihColumnValidations.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == validation.Key, cancellationToken: cancellationToken);
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
					validation.ResetKeys();
					validation.CopyProperties(dbColumnValidation, true);
					DbContext.DexihColumnValidations.Add(dbColumnValidation);
				}


				dbColumnValidation.HubKey = hubKey;
				dbColumnValidation.UpdateDate = DateTime.UtcNow;
				dbColumnValidation.IsValid = true;

				await SaveHubChangesAsync(hubKey, cancellationToken);

				return dbColumnValidation;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save column validation {validation.Name} failed.  {ex.Message}", ex);
            }
        }
		
	  	public async Task<DexihCustomFunction[]> DeleteCustomFunctions(long hubKey, long[] customFunctionKeys, CancellationToken cancellationToken)
		{
            try
            {
                var dbFunctions = await DbContext.DexihCustomFunctions
                    .Where(c => c.HubKey == hubKey && customFunctionKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var function in dbFunctions)
                {
	                function.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbFunctions;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete custom functions failed.  {ex.Message}", ex);
            }
		}

        public async Task<DexihCustomFunction> SaveCustomFunction(long hubKey, DexihCustomFunction function, CancellationToken cancellationToken)
		{
			try
			{
				DexihCustomFunction dbFunction;

				//check there are no connections with the same name
				var sameName = await DbContext.DexihCustomFunctions.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == function.Name && c.Key != function.Key && c.IsValid, cancellationToken: cancellationToken);
				if (sameName != null)
				{
                    throw new RepositoryManagerException($"A custom function with the name {function.Name} already exists in the hub.");
				}

				//if there is a connectionKey, retrieve the record from the database, and copy the properties across.
				if (function.Key > 0)
				{
					dbFunction = await DbContext.DexihCustomFunctions.Include(c=>c.DexihCustomFunctionParameters).SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == function.Key, cancellationToken: cancellationToken);
					
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
					function.ResetKeys();
					dbFunction = function.CloneProperties<DexihCustomFunction>(false);
					DbContext.DexihCustomFunctions.Add(dbFunction);
				}


				dbFunction.HubKey = hubKey;
				dbFunction.UpdateDate = DateTime.UtcNow;
				dbFunction.IsValid = true;

//			 var modifiedEntries = DbContext.ChangeTracker
//				.Entries()
//				.Where(x => x.State == EntityState.Modified || x.State == EntityState.Added || x.State == EntityState.Deleted)
//				.Select(x => x)
//				.ToList();
				
				await SaveHubChangesAsync(hubKey, cancellationToken);

				return dbFunction;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save custom function {function.Name} failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihFileFormat[]> DeleteFileFormats(long hubKey, long[] fileFormatKeys, CancellationToken cancellationToken)
		{
            try
            {
                var dbFileFormats = await DbContext.DexihFileFormats
                    .Where(c => c.HubKey == hubKey && fileFormatKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var fileformat in dbFileFormats)
                {
                    fileformat.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbFileFormats;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete file formats failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihFileFormat> SaveFileFormat(long hubKey, DexihFileFormat fileformat, CancellationToken cancellationToken)
		{
			try
			{
				DexihFileFormat dbFileFormat;

				//check there are no connections with the same name
				var sameName = await DbContext.DexihFileFormats.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == fileformat.Name && c.Key != fileformat.Key && c.IsValid, cancellationToken: cancellationToken);
				if (sameName != null)
				{
                    throw new RepositoryManagerException($"A file format with the name {fileformat.Name} already exists in the repository.");
				}

				//if there is a connectionKey, retrieve the record from the database, and copy the properties across.
				if (fileformat.Key > 0)
				{
					dbFileFormat = await DbContext.DexihFileFormats.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == fileformat.Key, cancellationToken: cancellationToken);
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
					fileformat.ResetKeys();
					fileformat.CopyProperties(dbFileFormat, true);
					DbContext.DexihFileFormats.Add(dbFileFormat);
				}

				dbFileFormat.UpdateDate = DateTime.UtcNow;
				dbFileFormat.IsValid = true;

				await SaveHubChangesAsync(hubKey, cancellationToken);

				return dbFileFormat;
			}
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save file format {fileformat.Name} failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihHubVariable[]> DeleteHubVariables(long hubKey, long[] hubVariableKeys, CancellationToken cancellationToken)
        {
            try
            {
                var dbHubVariables = await DbContext.DexihHubVariables
                    .Where(c => c.HubKey == hubKey && hubVariableKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var hubVariable in dbHubVariables)
                {
                    hubVariable.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbHubVariables;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete hub variables failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihHubVariable> SaveHubVariable(long hubKey, DexihHubVariable hubHubVariable, CancellationToken cancellationToken)
        {
            try
            {
                DexihHubVariable dbHubHubVariable;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihHubVariables.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == hubHubVariable.Name && c.Key != hubHubVariable.Key && c.IsValid, cancellationToken: cancellationToken);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A variable with the name {hubHubVariable.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (hubHubVariable.Key > 0)
                {
                    dbHubHubVariable = await DbContext.DexihHubVariables.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == hubHubVariable.Key, cancellationToken: cancellationToken);
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
                    hubHubVariable.ResetKeys();
                    hubHubVariable.CopyProperties(dbHubHubVariable, true);
                    DbContext.DexihHubVariables.Add(dbHubHubVariable);
                }

				dbHubHubVariable.UpdateDate = DateTime.UtcNow;
                dbHubHubVariable.IsValid = true;

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbHubHubVariable;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save file format {hubHubVariable.Name} failed.  {ex.Message}", ex);
            }
        }

	   public async Task<DexihView[]> DeleteViews(long hubKey, long[] viewKeys, CancellationToken cancellationToken)
        {
            try
            {
                var dbViews = await DbContext.DexihViews
                    .Where(c => c.HubKey == hubKey && viewKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var view in dbViews)
                {
	                view.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbViews;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete views failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihView> SaveView(long hubKey, DexihView view, CancellationToken cancellationToken)
        {
            try
            {
                DexihView dbView;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihViews.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == view.Name && c.Key != view.Key && c.IsValid, cancellationToken: cancellationToken);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A view with the name {view.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (view.Key > 0)
                {
	                dbView = await GetView(hubKey, view.Key, cancellationToken);
                    if (dbView != null)
                    {
	                    view.CopyProperties(dbView, false);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The view could not be saved as it contains the view_key {view.Key} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbView = new DexihView();
                    view.ResetKeys();
                    view.CopyProperties(dbView, false);
                    DbContext.DexihViews.Add(dbView);
                }


                dbView.UpdateDate = DateTime.UtcNow;
                dbView.IsValid = true;

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbView;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save view {view.Name} failed.  {ex.Message}", ex);
            }
        }
        
        
        public async Task<DexihDashboard[]> DeleteDashboards(long hubKey, long[] dashboardKeys, CancellationToken cancellationToken)
        {
            try
            {
                var dbDashboards = await DbContext.DexihDashboards
                    .Where(c => c.HubKey == hubKey && dashboardKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var dashboard in dbDashboards)
                {
	                dashboard.IsValid = false;

	                foreach (var item in dashboard.DexihDashboardItems)
	                {
		                item.IsValid = false;
	                }
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbDashboards;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete dashboards failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihDashboard> SaveDashboard(long hubKey, DexihDashboard dashboard, CancellationToken cancellationToken)
        {
            try
            {
                DexihDashboard dbDashboard;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihDashboards.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == dashboard.Name && c.Key != dashboard.Key && c.IsValid, cancellationToken: cancellationToken);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A dashboard with the name {dashboard.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (dashboard.Key > 0)
                {
	                dbDashboard = await GetDashboard(hubKey, dashboard.Key, cancellationToken);
                    if (dbDashboard != null)
                    {
	                    dashboard.CopyProperties(dbDashboard, false);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The dashboard could not be saved as it contains the key {dashboard.Key} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbDashboard = new DexihDashboard();
                    dashboard.ResetKeys();
                    dashboard.CopyProperties(dbDashboard, false);
                    DbContext.DexihDashboards.Add(dbDashboard);
                }

                dbDashboard.UpdateDate = DateTime.UtcNow;
                dbDashboard.IsValid = true;

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbDashboard;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save dashboard {dashboard.Name} failed.  {ex.Message}", ex);
            }
        }
        
          public async Task<DexihApi[]> DeleteApis(long hubKey, long[] apiKeys, CancellationToken cancellationToken)
        {
            try
            {
                var dbApis = await DbContext.DexihApis
                    .Where(c => c.HubKey == hubKey && apiKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var api in dbApis)
                {
	                api.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbApis;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete API's failed.  {ex.Message}", ex);
            }
        }

        public async Task<DexihApi> SaveApi(long hubKey, DexihApi api, CancellationToken cancellationToken)
        {
            try
            {
	            DexihApi dbApi;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihApis.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == api.Name && c.Key != api.Key && c.IsValid, cancellationToken: cancellationToken);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A API with the name {api.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (api.Key > 0)
                {
                    dbApi = await DbContext.DexihApis.Include(c => c.Parameters).SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == api.Key, cancellationToken: cancellationToken);
                    if (dbApi != null)
                    {
	                    api.CopyProperties(dbApi, false);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The api could not be saved as it contains the api_key {api.Key} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbApi = new DexihApi();
                    api.ResetKeys();
                    api.CopyProperties(dbApi, true);
                    DbContext.DexihApis.Add(dbApi);
                }

                dbApi.UpdateDate = DateTime.UtcNow;
                dbApi.IsValid = true;

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbApi;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save api {api.Name} failed.  {ex.Message}", ex);
            }
        }
		
		public async Task<DexihDatalinkTest[]> DeleteDatalinkTests(long hubKey, long[] datalinkTestKeys, CancellationToken cancellationToken)
        {
            try
            {
                var dbTests = await DbContext.DexihDatalinkTests
                    .Where(c => c.HubKey == hubKey && datalinkTestKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var item in dbTests)
                {
	                item.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbTests;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete hub tests failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihDatalinkTest> SaveDatalinkTest(long hubKey, DexihDatalinkTest datalinkTest, CancellationToken cancellationToken)
        {
            try
            {
	            DexihDatalinkTest dbDatalinkTest;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihDatalinkTests.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == datalinkTest.Name && c.Key != datalinkTest.Key && c.IsValid, cancellationToken: cancellationToken);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A test with the name {datalinkTest.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (datalinkTest.Key > 0)
                {
	                var cacheManager = new CacheManager(hubKey, "", _logger);
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
	                datalinkTest.ResetKeys();
	                datalinkTest.CopyProperties(dbDatalinkTest, false);
                    DbContext.DexihDatalinkTests.Add(dbDatalinkTest);
                }


	            dbDatalinkTest.UpdateDate = DateTime.UtcNow;	            
	            dbDatalinkTest.IsValid = true;

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbDatalinkTest;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save file format {datalinkTest.Name} failed.  {ex.Message}", ex);
            }
        }

		public async Task<DexihListOfValues[]> DeleteListOfValues(long hubKey, long[] listOfValuesKeys, CancellationToken cancellationToken)
        {
            try
            {
                var dbListOfValues = await DbContext.DexihListOfValues
                    .Where(c => c.HubKey == hubKey && listOfValuesKeys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var listOfValues in dbListOfValues)
                {
	                listOfValues.IsValid = false;
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbListOfValues;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete listOfValues failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihListOfValues> SaveListOfValues(long hubKey, DexihListOfValues listOfValues, CancellationToken cancellationToken)
        {
            try
            {
                DexihListOfValues dbListOfValues;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihListOfValues.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == listOfValues.Name && c.Key != listOfValues.Key && c.IsValid, cancellationToken: cancellationToken);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A listOfValues with the name {listOfValues.Name} already exists in the repository.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (listOfValues.Key > 0)
                {
	                dbListOfValues = await GetListOfValues(hubKey, listOfValues.Key, cancellationToken);
                    if (dbListOfValues != null)
                    {
	                    listOfValues.CopyProperties(dbListOfValues, false);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The listOfValues could not be saved as it contains the listOfValues_key {listOfValues.Key} that no longer exists in the repository.");
                    }
                }
                else
                {
                    dbListOfValues = new DexihListOfValues();
                    listOfValues.ResetKeys();
                    listOfValues.CopyProperties(dbListOfValues, true);
                    DbContext.DexihListOfValues.Add(dbListOfValues);
                }

                dbListOfValues.UpdateDate = DateTime.UtcNow;
                dbListOfValues.IsValid = true;

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbListOfValues;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save listOfValues {listOfValues.Name} failed.  {ex.Message}", ex);
            }
        }
        
        public async Task<DexihTag[]> DeleteTags(long hubKey, long[] keys, CancellationToken cancellationToken)
        {
            try
            {
                var dbTags = await DbContext.DexihTags
	                .Include(c => c.DexihTagObjects)
                    .Where(c => c.HubKey == hubKey && keys.Contains(c.Key) && c.IsValid)
                    .ToArrayAsync(cancellationToken: cancellationToken);

                foreach (var tag in dbTags)
                {
	                tag.IsValid = false;

	                foreach (var tagObject in tag.DexihTagObjects)
	                {
		                tagObject.IsValid = false;
	                }
                }

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbTags;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Delete tags failed.  {ex.Message}", ex);
            }

        }

        public async Task<DexihTag> SaveTag(long hubKey, DexihTag tag, CancellationToken cancellationToken)
        {
            try
            {
	            DexihTag dbTag;

                //check there are no connections with the same name
                var sameName = await DbContext.DexihTags.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.Name == tag.Name && c.Key != tag.Key && c.IsValid, cancellationToken: cancellationToken);
                if (sameName != null)
                {
                    throw new RepositoryManagerException($"A tag with the name {tag.Name} already exists in the hub.");
                }

                //if there is a connectionKey, retrieve the record from the database, and copy the properties across.
                if (tag.Key > 0)
                {
                    dbTag = await DbContext.DexihTags.SingleOrDefaultAsync(d => d.HubKey == hubKey && d.Key == tag.Key, cancellationToken: cancellationToken);
                    if (dbTag != null)
                    {
                        tag.CopyProperties(dbTag, true);
                    }
                    else
                    {
                        throw new RepositoryManagerException($"The variable could not be saved as it contains the hub_variable_key {tag.Key} that no longer exists in the hub.");
                    }
                }
                else
                {
                    dbTag = new DexihTag();
                    tag.ResetKeys();
                    tag.CopyProperties(dbTag, true);
                    DbContext.DexihTags.Add(dbTag);
                }

				dbTag.UpdateDate = DateTime.UtcNow;
                dbTag.IsValid = true;

                await SaveHubChangesAsync(hubKey, cancellationToken);

                return dbTag;
            }
            catch (Exception ex)
            {
                throw new RepositoryManagerException($"Save tag {tag.Name} failed.  {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Updates a TagObject with all the tags for a specified object key.  Missing tags will be marked invalid.
        /// </summary>
        /// <param name="hubKey"></param>
        /// <param name="objectKey"></param>
        /// <param name="objectType"></param>
        /// <param name="tagKeys"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task SaveObjectTags(long hubKey, long objectKey, ESharedObjectType objectType, long[] tagKeys, CancellationToken cancellationToken)
        {
	        var existing = await DbContext.DexihTagObjects
		        .Where(c => c.HubKey == hubKey && c.ObjectKey == objectKey && c.ObjectType == objectType)
		        .ToDictionaryAsync(c => c.TagKey, cancellationToken: cancellationToken);

	        foreach (var tagObject in existing.Values)
	        {
		        tagObject.IsValid = false;
	        }
	        
	        foreach(var tagKey in tagKeys)
	        {
		        if (existing.TryGetValue(tagKey, out var value))
		        {
			        value.IsValid = true;
			        value.UpdateDate = DateTime.Now;
		        }
		        else
		        {
			        var tagObject = new DexihTagObject()
			        {
				        ObjectKey = objectKey,
				        ObjectType = objectType,
				        TagKey = tagKey
			        };
			        DbContext.DexihTagObjects.Add(tagObject);
		        }
	        }

	        await SaveHubChangesAsync(hubKey, cancellationToken);
        }
        
        /// <summary>
        /// Updates a TagObject with all the objects for a specified tag key.  Missing tags will be marked invalid.
        /// </summary>
        /// <param name="hubKey"></param>
        /// <param name="objectKey"></param>
        /// <param name="objectType"></param>
        /// <param name="tagKeys"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task SaveTagObjects(long hubKey, long tagKey, bool isChecked, ObjectTypeKey[] objectKeys, CancellationToken cancellationToken)
        {
	        var existing = await DbContext.DexihTagObjects
		        .Where(c => c.HubKey == hubKey && c.TagKey == tagKey)
		        .ToDictionaryAsync(c => 
		        new ObjectTypeKey {ObjectType = c.ObjectType, ObjectKey = c.ObjectKey}, cancellationToken: cancellationToken);
	        
	        foreach(var objectKey in objectKeys)
	        {
		        if (existing.TryGetValue(objectKey, out var value))
		        {
			        value.IsValid = isChecked;
			        value.UpdateDate = DateTime.Now;
		        }
		        else
		        {
			        if (isChecked)
			        {
				        var tagObject = new DexihTagObject()
				        {
					        ObjectKey = objectKey.ObjectKey,
					        ObjectType = objectKey.ObjectType,
					        TagKey = tagKey
				        };
				        DbContext.DexihTagObjects.Add(tagObject);
			        }
		        }
	        }

	        await SaveHubChangesAsync(hubKey, cancellationToken);
        }
        
        public async Task DeleteTagObjects(long hubKey, ObjectTypeKey[] objectKeys, CancellationToken cancellationToken)
        {
	        // get the objectKeys and use them to limit the pushdown SQL
	        // NOTE: Can't workout how to include objectKey and objectType in the pushdown
	        var keys = objectKeys.Select(c => c.ObjectKey).ToArray();
	        var existing = await DbContext.DexihTagObjects
		        .Where(c => c.HubKey == hubKey && keys.Contains(c.ObjectKey))
		        .ToArrayAsync(cancellationToken: cancellationToken);
	        
	        foreach(var tagObject in existing)
	        {
		        // check objecttype as the prefilter on object keys will cause problems where different objecttypes have same key
		        if (objectKeys.Any(c => c.ObjectKey == tagObject.ObjectKey && c.ObjectType == tagObject.ObjectType))
		        {
			        tagObject.IsValid = false;
			        tagObject.UpdateDate = DateTime.Now;
		        }
	        }

	        await SaveHubChangesAsync(hubKey, cancellationToken);
        }
        
        /// <summary>
        /// Creates a new datalink test with a separate step for each of the specified datalinkKeys.
        /// </summary>
        /// <param name="hubKey"></param>
        /// <param name="hub"></param>
        /// <param name="name"></param>
        /// <param name="datalinkKeys">Array containing datalink keys to add to the test.</param>
        /// <param name="auditConnectionKey"></param>
        /// <param name="targetConnectionKey"></param>
        /// <param name="sourceConnectionKey"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<DexihDatalinkTest> NewDatalinkTest(long hubKey, DexihHub hub, string name, long[] datalinkKeys, long auditConnectionKey, long targetConnectionKey, long sourceConnectionKey, CancellationToken cancellationToken)
		{
			var datalinks = hub.DexihDatalinks.Where(c => datalinkKeys.Contains(c.Key)).ToArray();
			
			var datalinkTest = new DexihDatalinkTest
			{
				Name = string.IsNullOrEmpty(name) ? datalinks.Length == 1 ? $"{datalinks[0].Name} tests" : "datalink tests" : name,
				AuditConnectionKey = auditConnectionKey
			};

			var uniqueId = ShortGuid.NewGuid().ToString().Replace("-", "_");

			foreach (var datalink in datalinks)
			{
				//TODO Update datalink test to allow for multiple target tables.
				
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
					TargetSchema = "",
					TargetConnectionKey = targetConnectionKey,
					ErrorTableName = $"{targetName}_error_{uniqueId}",
					ErrorSchema = "",
					ErrorConnectionKey = targetConnectionKey
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
						Action = ETestTableAction.DropCreateCopy,
					};
					datalinkStep.DexihDatalinkTestTables.Add(testTable);
				}

				datalinkTest.DexihDatalinkTestSteps.Add(datalinkStep);
			}
			
//			var modifiedEntries = DbContext.ChangeTracker
//				.Entries()
//				.Where(x => x.State == EntityState.Modified || x.State == EntityState.Added || x.State == EntityState.Deleted)
//				.Select(x => x)
//				.ToList();

			DbContext.DexihDatalinkTests.Add(datalinkTest);
			
			await SaveHubChangesAsync(hubKey, cancellationToken);

			return datalinkTest;
		}

        private async Task<(Dictionary<long, long> keyMappings, ImportObjects<T> importObjects)> AddImportItems<T>(long hubKey, ICollection<T> items, IQueryable<T> dbItems, EImportAction importAction) where T : DexihHubNamedEntity
        {
	        var keyMappings = new Dictionary<long, long>();
	        var importObjects = new ImportObjects<T>();
	        var keySequence = -1;
	        
	        foreach (var item in items)
	        {
		        
		        item.HubKey = hubKey;
		        var matchingItems = await dbItems.Where(var => var.HubKey == hubKey && var.Name == item.Name && var.IsValid).ToArrayAsync();
		        matchingItems = matchingItems.Where(var => var.ParentKey == item.ParentKey).ToArray();

		        if (matchingItems.Any())
		        {
			        if (matchingItems.Length > 1)
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
					        item.Name = item.Name + " - duplicate rename " + DateTime.UtcNow;
					        importObjects.Add(item, EImportAction.New);
					        break;
				        case EImportAction.Leave:
				        case EImportAction.Skip:
					        break;
				        default:
					        throw new ArgumentOutOfRangeException(nameof(importAction), importAction, null);
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
        
        private void UpdateTableColumns(long hubKey, ICollection<DexihTableColumn> childItems, ICollection<DexihTableColumn> existingItems, EImportAction importAction, Dictionary<long, long> mappings, ref int keySequence)
        {
	        foreach (var childItem in childItems)
	        {
		        childItem.HubKey = hubKey;
		        var existingItem = existingItems?.SingleOrDefault(var => childItem.Name == var.Name);

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
					        throw new ArgumentOutOfRangeException(nameof(importAction), importAction, null);
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
			        if (existingItems == null)
			        {
				        throw new RepositoryManagerException("Cannot replace table columns ad existing items is null.");
			        }
			        
			        var deleteItems =
				        existingItems.Where(c => c.IsValid && !childItems.Select(t => t.Key).Contains(c.Key));
			        foreach (var deleteItem in deleteItems)
			        {
				        deleteItem.IsValid = false;
			        }
		        }

		        if (childItem.ChildColumns != null)
		        {
			        UpdateTableColumns(hubKey, childItem.ChildColumns, existingItem?.ChildColumns, importAction, mappings, ref keySequence);
		        }
	        }
        }

        /// <summary>
        /// Compares an imported hub structure against the database structure, and maps keys and dependent objects together.
        /// </summary>
        /// <param name="hubKey"></param>
        /// <param name="hub"></param>
        /// <param name="importActions"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public async Task<Import> CreateImportPlan(long hubKey, DexihHub hub, ImportAction[] importActions, CancellationToken cancellationToken)
		{
			var keySequence = -1;
			var plan = new Import(hubKey);

			var actions = importActions.ToDictionary(c => c.ObjectType, c => c.Action);
			
			// add all the top level shared objects 
			var hubVariables = await AddImportItems(hubKey, hub.DexihHubVariables, DbContext.DexihHubVariables, actions[ESharedObjectType.HubVariable]);
			var connections = await AddImportItems(hubKey, hub.DexihConnections, DbContext.DexihConnections, actions[ESharedObjectType.Connection]);
			var datajobs = await AddImportItems(hubKey, hub.DexihDatajobs, DbContext.DexihDatajobs, actions[ESharedObjectType.Datajob]);
			var datalinks = await AddImportItems(hubKey, hub.DexihDatalinks, DbContext.DexihDatalinks, actions[ESharedObjectType.Datalink]);
			var columnValidations = await AddImportItems(hubKey, hub.DexihColumnValidations, DbContext.DexihColumnValidations, actions[ESharedObjectType.ColumnValidation]);
			var customFunctions = await AddImportItems(hubKey, hub.DexihCustomFunctions, DbContext.DexihCustomFunctions, actions[ESharedObjectType.CustomFunction]);
			var fileFormats = await AddImportItems(hubKey, hub.DexihFileFormats, DbContext.DexihFileFormats, actions[ESharedObjectType.FileFormat]);
			var apis = await AddImportItems(hubKey, hub.DexihApis, DbContext.DexihApis, actions[ESharedObjectType.Api]);
			var views = await AddImportItems(hubKey, hub.DexihViews, DbContext.DexihViews, actions[ESharedObjectType.View]);
			var dashboards = await AddImportItems(hubKey, hub.DexihDashboards, DbContext.DexihDashboards, actions[ESharedObjectType.Dashboard]);
			var datalinkTests = await AddImportItems(hubKey, hub.DexihDatalinkTests, DbContext.DexihDatalinkTests, actions[ESharedObjectType.DatalinkTest]);
			var listOfValues = await AddImportItems(hubKey, hub.DexihListOfValues, DbContext.DexihListOfValues, actions[ESharedObjectType.ListOfValues]);
			var tags = await AddImportItems(hubKey, hub.DexihTags, DbContext.DexihTags, actions[ESharedObjectType.Tags]);

			// update the table connection keys to the target connection keys, as these are required updated before matching
			foreach (var table in hub.DexihTables)
			{
				table.ConnectionKey = UpdateConnectionKey(table.ConnectionKey) ?? 0;
			}
			var tables = await AddImportItems(hubKey, hub.DexihTables, DbContext.DexihTables, actions[ESharedObjectType.Table]);
			
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
			plan.Dashboards = dashboards.importObjects;
			plan.DatalinkTests = datalinkTests.importObjects;
			plan.ListOfValues = listOfValues.importObjects;
			plan.Tags = tags.importObjects;

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

			long? UpdateViewKey(long? key)
			{
				if (key != null)
				{
					if (views.keyMappings.TryGetValue(key.Value, out var newKey))
					{
						return newKey;	
					}
					else
					{
						plan.Warnings.Add($"The view key {key} does not exist in the package.  This will need to be manually fixed after import.");
						return null;
					}
				}

				return null;
			}

			long? UpdateListOfValuesKey(long? key)
			{
				if (key != null)
				{
					if (listOfValues.keyMappings.TryGetValue(key.Value, out var newKey))
					{
						return newKey;	
					}
					else
					{
						plan.Warnings.Add($"The list of values key {key} does not exist in the package.  This will need to be manually fixed after import.");
						return null;
					}
				}

				return null;
			}
			
			// add the table column mappings
			var tableColumnMappings = new Dictionary<long, long>();
			var dbTableColumns = await DbContext.DexihTableColumns.Where(c => c.TableKey != null && tables.keyMappings.Keys.Contains(c.TableKey.Value) && c.IsValid).ToArrayAsync(cancellationToken: cancellationToken);
			foreach (var table in tables.importObjects)
			{
				var existingItems = dbTableColumns.Where(c => c.TableKey == table.Item.Key).ToArray();
				UpdateTableColumns(hubKey, table.Item.DexihTableColumns, existingItems, table.ImportAction, tableColumnMappings, ref keySequence);

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

                void UpdateChildColumns(ICollection<DexihTableColumn> columns, DexihTableColumn parentColumn)
                {
	                foreach (var column in columns)
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

		                if (parentColumn == null)
		                {
			                column.ParentColumnKey = null;
			                column.ParentColumn = null;
		                }
		                else
		                {
			                column.ParentColumnKey = parentColumn.ParentColumnKey;
			                column.ParentColumn = parentColumn;
		                }

		                if (column.ChildColumns != null)
		                {
			                UpdateChildColumns(column.ChildColumns, column);
		                }
	                }
                }
                
                UpdateChildColumns(table.Item.DexihTableColumns, null);
                
//				foreach (var column in table.Item.DexihTableColumns)
//				{
//					if (column.ColumnValidationKey != null)
//					{
//						if (columnValidations.keyMappings.TryGetValue(column.ColumnValidationKey.Value, out var columnValidationKey))
//						{
//							column.ColumnValidationKey = columnValidationKey;
//						}
//						else
//						{
//							plan.Warnings.Add($"The column {table.Item.Name}.{column.Name} contains a validation with the key {column.ColumnValidationKey.Value} which does not exist in the package.  This will need to be manually fixed after import.");
//							column.ColumnValidationKey = null;
//						}
//					}
//				}
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
				if (view.SourceType == EDataObjectType.Datalink && view.SourceDatalinkKey != null)
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
				
				if (view.SourceType == EDataObjectType.Table && view.SourceTableKey != null)
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

				foreach (var parameter in view.Parameters)
				{
					parameter.ListOfValuesKey = UpdateListOfValuesKey(parameter.ListOfValuesKey);
				}
			}

			foreach (var dashboard in dashboards.importObjects.Select(c => c.Item))
			{
				foreach (var item in dashboard.DexihDashboardItems)
				{
					item.ViewKey = UpdateViewKey(item.ViewKey) ?? default;
				}
				
				foreach (var parameter in dashboard.Parameters)
				{
					parameter.ListOfValuesKey = UpdateListOfValuesKey(parameter.ListOfValuesKey);
				}
			}

			foreach (var lovItem in listOfValues.importObjects.Select(c => c.Item))
			{
				if (lovItem.SourceType == ELOVObjectType.Datalink && lovItem.SourceDatalinkKey != null)
				{
					if (datalinks.keyMappings.TryGetValue(lovItem.SourceDatalinkKey.Value,
						out var sourceDatalinkKey))
					{
						lovItem.SourceDatalinkKey = sourceDatalinkKey;	
					}
					else
					{
						plan.Warnings.Add($"The list of values {lovItem.Name} contains the source datalink with the key {lovItem.SourceDatalinkKey} which does not exist in the package.  This will need to be manually fixed after import.");
						lovItem.SourceDatalinkKey = null;
					}
				}
				
				if (lovItem.SourceType == ELOVObjectType.Table && lovItem.SourceTableKey != null)
				{
					if (tables.keyMappings.TryGetValue(lovItem.SourceTableKey.Value, out var sourceTableKey))
					{
						lovItem.SourceTableKey = sourceTableKey;
					}
					else
					{
						plan.Warnings.Add($"The list of values {lovItem.Name} contains the source table with the key {lovItem.SourceTableKey} which does not exist in the package.  This will need to be manually fixed after import.");
						lovItem.SourceTableKey = null;
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
				
				foreach (var parameter in api.Parameters)
				{
					parameter.ListOfValuesKey = UpdateListOfValuesKey(parameter.ListOfValuesKey);
				}
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
				
				// resets columns in datalink table.  Used as a function as it is required for the sourceDatalink and joinDatalink.
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
					target.Key = 0;
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

				foreach (var profile in datalink.DexihDatalinkProfiles)
				{
					profile.Key = 0;
				}

				foreach (var parameter in datalink.Parameters)
				{
					parameter.ListOfValuesKey = UpdateListOfValuesKey(parameter.ListOfValuesKey);
				}


				foreach (var datalinkTransform in datalink.DexihDatalinkTransforms.OrderBy(c=>c.Position))
				{
					long? DatalinkColumnMapping(DexihDatalinkColumn datalinkColumn)
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

					datalinkTransform.NodeDatalinkColumnKey = DatalinkColumnMapping(datalinkTransform.NodeDatalinkColumn);
					datalinkTransform.NodeDatalinkColumn = null;

					datalinkTransform.JoinSortDatalinkColumnKey = DatalinkColumnMapping(datalinkTransform.JoinSortDatalinkColumn);
					datalinkTransform.JoinSortDatalinkColumn = null;
					
					foreach (var item in datalinkTransform.DexihDatalinkTransformItems.OrderBy(c => c.Position).Where(c => c.IsValid))
					{
						item.Key = 0;
						item.HubKey = hubKey;

						item.JoinDatalinkColumnKey = DatalinkColumnMapping(item.JoinDatalinkColumn);
						item.JoinDatalinkColumn = null;

						item.SourceDatalinkColumnKey = DatalinkColumnMapping(item.SourceDatalinkColumn);
						item.SourceDatalinkColumn = null;
						
						item.FilterDatalinkColumnKey = DatalinkColumnMapping(item.FilterDatalinkColumn);
						item.FilterDatalinkColumn = null;

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
								parameter.DatalinkColumnKey = DatalinkColumnMapping(parameter.DatalinkColumn);
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
										arrayParam.DatalinkColumnKey = DatalinkColumnMapping(arrayParam.DatalinkColumn);
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
				foreach (var parameter in datajob.Parameters)
				{
					parameter.Key = 0;
					parameter.HubKey = hubKey;
					parameter.DatajobKey = 0;
					parameter.ListOfValuesKey = UpdateListOfValuesKey(parameter.ListOfValuesKey);
				}

				
				foreach (var trigger in datajob.DexihTriggers)
				{
					trigger.Key = 0;
					trigger.HubKey = hubKey;
				}

				var stepKeyMapping = new Dictionary<long, DexihDatalinkStep>();

				// var newSteps = new HashSet<DexihDatalinkStep>();
				
				foreach (var step in datajob.DexihDatalinkSteps)
				{
					step.HubKey = hubKey;
					step.DatajobKey = 0;
					
					foreach (var parameter in datajob.Parameters)
					{
						parameter.ListOfValuesKey = UpdateListOfValuesKey(parameter.ListOfValuesKey);
					}

					stepKeyMapping.Add(step.Key, step);
					//step.Key = 0;

					step.DatalinkKey = UpdateDatalinkKey(step.DatalinkKey);
				}
				// datajob.DexihDatalinkSteps = newSteps;
				
				foreach (var step in datajob.DexihDatalinkSteps)
				{
					foreach (var dep in step.DexihDatalinkDependencies)
					{
						dep.DependentDatalinkStep = stepKeyMapping.GetValueOrDefault(dep.DependentDatalinkStepKey);
						// dep.Key = 0;
						dep.DatalinkStepKey = 0;
						// dep.DependentDatalinkStepKey = 0;
					}

					step.DexihDatalinkDependentSteps = null;
				}

				datajob.AuditConnectionKey = UpdateConnectionKey(datajob.AuditConnectionKey);
			}
			
			return plan;
		}

        void AddChildColumns(Dictionary<long, DexihTableColumn> columns, DexihTableColumn column)
        {
	        if (column.ChildColumns != null && column.ChildColumns.Count > 0)
	        {
		        foreach (var childColumn in column.ChildColumns)
		        {
			        columns.Add(childColumn.Key, childColumn);
			        AddChildColumns(columns, childColumn);
		        }
	        }
        }



        /// <summary>
        /// Imports are package into the current repository.
        /// </summary>
        /// <param name="import"></param>
        /// <param name="allowPasswordImport">Allows the import to import passwords/connection strings</param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task ImportPackage(Import import, bool allowPasswordImport, CancellationToken cancellationToken)
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
            var columns = import.Tables.Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace)
	            .SelectMany(c => c.Item.DexihTableColumns).ToDictionary(c => c.Key, c => c);
            var datalinks = import.Datalinks
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.Key, c => c);
            var datajobs = import.Datajobs
                .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
                .ToDictionary(c => c.Key, c => c);
            var datalinkTests = import.DatalinkTests
	            .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
	            .ToDictionary(c => c.Key, c => c);
            var apis = import.Apis
	            .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
	            .ToDictionary(c => c.Key, c => c);
            var views = import.Views
	            .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
	            .ToDictionary(c => c.Key, c => c);
            var dashboards = import.Dashboards
	            .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
	            .ToDictionary(c => c.Key, c => c);
            var listOfValues = import.ListOfValues
	            .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
	            .ToDictionary(c => c.Key, c => c);
            var tags = import.Tags
	            .Where(c => c.ImportAction == EImportAction.New || c.ImportAction == EImportAction.Replace).Select(c => c.Item)
	            .ToDictionary(c => c.Key, c => c);
            
            // recurse the child columns and include any in the dictionary.
            var topColumns = columns.Values.ToArray();
            foreach (var column in topColumns)
            {
	            AddChildColumns(columns, column);
            }
	            
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
					// connection.ConnectionStringRaw = "";
					connection.UseConnectionStringVariable = false;
					connection.Password = "";
					// connection.PasswordRaw = "";
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

				if (columnValidation.LookupColumnKey != null)
				{
					columnValidation.LookupColumn = columns.GetValueOrDefault(columnValidation.LookupColumnKey.Value);
				}
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

                ResetKeys(datalink.Parameters);

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
                        item.FilterDatalinkColumn = item.FilterDatalinkColumnKey == null ? null : datalinkColumns.GetValueOrDefault(item.FilterDatalinkColumnKey.Value);

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

				ResetKeys(datajob.DexihTriggers);
				ResetKeys(datajob.Parameters);

				var steps = datajob.DexihDatalinkSteps.ToDictionary(c => c.Key, c => c);

				foreach (var step in steps.Values)
				{
					step.Key = 0;
                    step.Datalink =  datalinks.GetValueOrDefault(step.DatalinkKey.Value);
                    ResetKeys(step.Parameters);

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
            
            foreach(var datalinkTest in datalinkTests.Values)
            {
	            if (datalinkTest.Key < 0) datalinkTest.Key = 0;

	            // TODO Need to update datalink test import.
	            
//	            if (datalinkTest.AuditConnectionKey != null)
//	            {
//		            datalinkTest.AuditConnection = connections.GetValueOrDefault(datalinkTest.AuditConnectionKey.Value);
//	            }
//
//	            foreach (var step in datalinkTest.DexihDatalinkTestSteps)
//	            {
//		            if (step.ExpectedConnectionKey != null)
//		            {
//			            datalinkTest.AuditConnection = connections.GetValueOrDefault(step.ExpectedConnectionKey.Value);
//		            }
//	            }
            }
            
            foreach(var view in views.Values)
            {
	            if (view.Key < 0) view.Key = 0;

	            if (view.SourceDatalinkKey != null)
	            {
		            view.SourceDatalink = datalinks.GetValueOrDefault(view.SourceDatalinkKey.Value);
	            }

	            if (view.SourceTableKey != null)
	            {
		            view.SourceTable = tables.GetValueOrDefault(view.SourceTableKey.Value);
	            }
	            
	            ResetKeys(view.Parameters);
            }

            foreach (var dashboard in dashboards.Values)
            {
	            if (dashboard.Key < 0) dashboard.Key = 0;

	            foreach (var item in dashboard.DexihDashboardItems)
	            {
		            item.View = views.GetValueOrDefault(item.ViewKey);
	            }
	            
	            ResetKeys(dashboard.Parameters);

            }

            foreach(var lovItem in listOfValues.Values)
            {
	            if (lovItem.Key < 0) lovItem.Key = 0;

	            if (lovItem.SourceDatalinkKey != null)
	            {
		            lovItem.SourceDatalink = datalinks.GetValueOrDefault(lovItem.SourceDatalinkKey.Value);
	            }

	            if (lovItem.SourceTableKey != null)
	            {
		            lovItem.SourceTable = tables.GetValueOrDefault(lovItem.SourceTableKey.Value);
	            }
            }
            
            foreach(var tag in tags.Values)
            {
	            if (tag.Key < 0) tag.Key = 0;
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
				transform.UpdateDate = DateTime.UtcNow;

				foreach (var item in transform.DexihDatalinkTransformItems)
				{
					item.IsValid = false;
					item.UpdateDate = DateTime.UtcNow;

					foreach (var param in item.DexihFunctionParameters)
					{
						param.IsValid = false;
						param.UpdateDate = DateTime.UtcNow;
					}
				}
			}
			
			// set unused trigger parameters to invalid
			var targets = datalinks.Values.SelectMany(c => c.DexihDatalinkTargets).Select(c => c.Key);
			var deletedTargets = DbContext.DexihDatalinkTargets.Where(s =>
				s.HubKey == import.HubKey &&
				datalinks.Values.Select(d => d.Key).Contains(s.DatalinkKey) && !targets.Contains(s.Key));
			foreach (var target in deletedTargets)
			{
				target.IsValid = false;
				target.UpdateDate = DateTime.UtcNow;
			}


			var dataLinkSteps = datajobs.Values.SelectMany(c => c.DexihDatalinkSteps).Select(c => c.Key);
			var deletedSteps = DbContext.DexihDatalinkStep.Where(s =>
             	s.HubKey == import.HubKey &&
				datajobs.Values.Select(d => d.Key).Contains(s.DatajobKey) && !dataLinkSteps.Contains(s.Key));
			foreach (var step in deletedSteps)
			{
				step.IsValid = false;
				step.UpdateDate = DateTime.UtcNow;

				foreach (var dep in step.DexihDatalinkDependencies)
				{
					dep.IsValid = false;
					dep.UpdateDate = DateTime.UtcNow;
				}
			}
			
			// set unused trigger parameters to invalid
			var triggers = datajobs.Values.SelectMany(c => c.DexihTriggers).Select(c => c.Key);
			var deletedTriggers = DbContext.DexihTriggers.Where(s =>
				s.HubKey == import.HubKey &&
				datajobs.Values.Select(d => d.Key).Contains(s.DatajobKey) && !triggers.Contains(s.Key));
			foreach (var trigger in deletedTriggers)
			{
				trigger.IsValid = false;
				trigger.UpdateDate = DateTime.UtcNow;
			}

			// set unused datajob parameters to invalid
			var parameterKeys = datajobs.Values.SelectMany(c => c.Parameters).Select(c => c.Key);
			var deletedDatajobParameters = DbContext.DexihDatajobParameters.Where(s =>
				s.HubKey == import.HubKey && datajobs.Values.Select(d => d.Key).Contains(s.DatajobKey) &&
				!parameterKeys.Contains(s.Key));
			foreach (var parameter in deletedDatajobParameters)
			{
				parameter.IsValid = false;
				parameter.UpdateDate = DateTime.UtcNow;
			}
			
			// set datalink parameters to invalid
			parameterKeys = datalinks.Values.SelectMany(c => c.Parameters).Select(c => c.Key);
			var deletedDatalinkParameters = DbContext.DexihDatalinkParameters.Where(s =>
				s.HubKey == import.HubKey && datalinks.Values.Select(d => d.Key).Contains(s.DatalinkKey) &&
				!parameterKeys.Contains(s.Key));
			foreach (var parameter in deletedDatalinkParameters)
			{
				parameter.IsValid = false;
				parameter.UpdateDate = DateTime.UtcNow;
			}

			// set unused view parameters to invalid
			parameterKeys = views.Values.SelectMany(c => c.Parameters).Select(c => c.Key);
			var deletedViewParameters = DbContext.DexihViewParameters.Where(s =>
				s.HubKey == import.HubKey && views.Values.Select(d => d.Key).Contains(s.ViewKey) &&
				!parameterKeys.Contains(s.Key));
			foreach (var parameter in deletedViewParameters)
			{
				parameter.IsValid = false;
				parameter.UpdateDate = DateTime.UtcNow;
			}
			
			// set unused dashboard parameters to invalid
			parameterKeys = dashboards.Values.SelectMany(c => c.Parameters).Select(c => c.Key);
			var deletedDashboardParameters = DbContext.DexihDashboardParameters.Where(s =>
				s.HubKey == import.HubKey && views.Values.Select(d => d.Key).Contains(s.DashboardKey) &&
				!parameterKeys.Contains(s.Key));
			foreach (var parameter in deletedDashboardParameters)
			{
				parameter.IsValid = false;
				parameter.UpdateDate = DateTime.UtcNow;
			}
			
			// set unused api parameters to invalid
			parameterKeys = apis.Values.SelectMany(c => c.Parameters).Select(c => c.Key);
			var deletedApiParameters = DbContext.DexihApiParameters.Where(s =>
				s.HubKey == import.HubKey && apis.Values.Select(d => d.Key).Contains(s.ApiKey) &&
				!parameterKeys.Contains(s.Key));
			foreach (var parameter in deletedApiParameters)
			{
				parameter.IsValid = false;
				parameter.UpdateDate = DateTime.UtcNow;
			}
			
			// get all columns from the repository that need to be removed.
			var allColumnKeys = tables.Values.SelectMany(t => t.DexihTableColumns).Select(c=>c.Key).Where(c => c > 0);
			var deletedColumns = DbContext.DexihTableColumns.Where(c =>
				c.HubKey == import.HubKey &&  c.TableKey != null &&
				tables.Values.Select(t => t.Key).Contains(c.TableKey.Value) && !allColumnKeys.Contains(c.Key)).Include(c=>c.ChildColumns);

			foreach (var column in deletedColumns)
			{
				column.IsValid = false;
				column.UpdateDate = DateTime.UtcNow;
			}

//			await DbContext.AddRangeAsync(hubVariables.Values, cancellationToken);
//			await DbContext.AddRangeAsync(columnValidations.Values, cancellationToken);
//			await DbContext.AddRangeAsync(fileFormats.Values, cancellationToken);
//			await DbContext.AddRangeAsync(connections.Values, cancellationToken);
//			await DbContext.AddRangeAsync(tables.Values, cancellationToken);
//			await DbContext.AddRangeAsync(datalinks.Values, cancellationToken);
//			await DbContext.AddRangeAsync(datajobs.Values, cancellationToken);
//			await DbContext.AddRangeAsync(datalinkTests.Values, cancellationToken);
//			await DbContext.AddRangeAsync(apis.Values, cancellationToken);
//			await DbContext.AddRangeAsync(views.Values, cancellationToken);
//			await DbContext.AddRangeAsync(dashboards.Values, cancellationToken);
//
//			var entries = DbContext.ChangeTracker.Entries()
//				.Where(e => e.State == EntityState.Added || e.State == EntityState.Modified);
//			
//			foreach (var entry in entries)
//			{
//				var item = entry.Entity;
//				var properties = item.GetType().GetProperties();
//				foreach (var property in properties)
//				{
//					foreach (var attrib in property.GetCustomAttributes(true))
//					{
//						if (attrib is CopyCollectionKeyAttribute)
//						{
//							var value = property.GetValue(item);
//							if (value != null && value is long valueLong)
//							{
//								entry.State = valueLong <= 0 ? EntityState.Added : EntityState.Modified;	
//							}
//						}
//					}
//				}
//			}

			AddItems(DbContext, hubVariables.Values);
			AddItems(DbContext, columnValidations.Values);
			AddItems(DbContext, fileFormats.Values);
			AddItems(DbContext, connections.Values);
			AddItems(DbContext, tables.Values);
			AddItems(DbContext, datalinks.Values);
			AddItems(DbContext, datajobs.Values);
			AddItems(DbContext, datalinkTests.Values);
			AddItems(DbContext, apis.Values);
			AddItems(DbContext, views.Values);
			AddItems(DbContext, dashboards.Values);
			AddItems(DbContext, listOfValues.Values);
			AddItems(DbContext, tags.Values);
			
			await SaveHubChangesAsync(import.HubKey, cancellationToken);

		}

        void ResetKeys(IEnumerable<DexihHubKeyEntity> entities)
        {
	        foreach (var entity in entities)
	        {
		        entity.Key = 0;
	        }
        }

        void AddItems(DbContext dbContext, IEnumerable<object> items)
        {
	        foreach (var item in items)
	        {
		        if (item is DexihHubNamedEntity entity)
		        {
			        if (entity.Key > 0)
			        {
				        dbContext.Update(item);
			        }
			        else
			        {
				        dbContext.Add(item);
			        }
		        }
		        else
		        {
			        dbContext.Add(item);
		        }
	        }
        }

    }
	


}

