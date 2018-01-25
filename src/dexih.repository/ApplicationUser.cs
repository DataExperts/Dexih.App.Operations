using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.AspNetCore.Identity;

namespace dexih.repository
{
    // Add profile data for application users by adding properties to the ApplicationUser class
    public class ApplicationUser : IdentityUser
    {
        [NotMapped]
        public bool IsAdmin { get; set; }
        [NotMapped]
        public bool IsManager { get; set; }
        [NotMapped]
        public bool IsUser { get; set; }
        [NotMapped]
        public bool IsViewer {get;set;}

		public bool IsInvited { get; set; }
		public bool IsRegistered { get; set; }
		public bool IsEnabled { get; set; }

		public string FirstName { get; set; }
		public string LastName { get; set; }
		public bool Terms { get; set; }
		public bool Subscription { get; set; }

		public int InviteQuota { get; set; }
		public int HubQuota { get; set; }

		public bool CanLogin()
		{
			return EmailConfirmed && IsInvited && IsEnabled;
		}

		public async Task<DexihHub[]> AuthorizedHubs(DexihRepositoryContext context, bool isAdmin = false)
        {
            if(!EmailConfirmed ) {
                return new DexihHub[0];
            }

            if (isAdmin)
            {
                return await context.DexihHubs.Where(c => c.IsValid).ToArrayAsync();
            }
            else
            {
                var hubs = await context.DexihHubUser.Where(c => c.UserId == Id && c.IsValid).Select(c => c.HubKey).ToArrayAsync();
                return await context.DexihHubs.Where(c => hubs.Contains(c.HubKey) && c.IsValid).ToArrayAsync();
            }
        }

        public async Task<DexihHubUser.EPermission> ValidateHub(DexihRepositoryContext context, long hubKey, bool isAdmin = false)
        {
            if(!EmailConfirmed) {
                throw new ApplicationUserException("The users email address has not been confirmed.");
            }

            var hub = await context.DexihHubs.SingleOrDefaultAsync(c => c.HubKey == hubKey);

            if (hub == null)
            {
                throw new ApplicationUserException("The hub with the key: " + hubKey + " could not be found.");
            }

            if (isAdmin)
            {
                return DexihHubUser.EPermission.Owner;
            }

            var hubUser = await context.DexihHubUser.FirstOrDefaultAsync(c => c.HubKey == hubKey && c.UserId == Id);

            if (hubUser.Permission == DexihHubUser.EPermission.Suspended || hubUser.Permission == DexihHubUser.EPermission.None)
            {
                throw new ApplicationUserException($"The users does not have access to the hub with key {hubKey}.");
            }
            else
            {
                return hubUser.Permission;
            }
        }
    }


}
