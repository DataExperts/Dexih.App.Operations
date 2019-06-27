using System;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.AspNetCore.Identity;

namespace dexih.repository
{
    // Add profile data for application users by adding properties to the ApplicationUser class
    [Serializable]
    public class ApplicationUser : IdentityUser
    {
    
	    public enum EUserRole
	    {
		    Administrator, Manager, User, Viewer, None
	    }

	    [NotMapped] public EUserRole UserRole { get; set; } = EUserRole.None;

	    [NotMapped] public bool IsAdmin => UserRole == EUserRole.Administrator;
	    [NotMapped] public bool IsManager => UserRole == EUserRole.Manager;
        [NotMapped] public bool IsUser => UserRole == EUserRole.User;
	    [NotMapped] public bool IsViewer => UserRole == EUserRole.Viewer;

		public bool IsInvited { get; set; }
		public bool IsRegistered { get; set; }
		public bool IsEnabled { get; set; }

		public string FirstName { get; set; }
		public string LastName { get; set; }
		public bool Terms { get; set; }
		public bool Subscription { get; set; }

		public int InviteQuota { get; set; }
		public int HubQuota { get; set; }

	    public string PrivateKey { get; set; }
	    public string CertificateChain { get; set; }
	    public DateTime? CertificateExpiry { get; set; }

		public bool CanLogin()
		{
			return EmailConfirmed && IsInvited && IsEnabled;
		}
    }
}
