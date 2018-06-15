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

    }


}
