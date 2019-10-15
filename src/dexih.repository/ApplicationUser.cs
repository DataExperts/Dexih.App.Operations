using System;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.AspNetCore.Identity;
using MessagePack;

namespace dexih.repository
{
    // Add profile data for application users by adding properties to the ApplicationUser class
    [MessagePackObject]
    public class ApplicationUser : IdentityUser
    {
	    

	    [NotMapped]
        [IgnoreMember]
        public EUserRole UserRole { get; set; } = EUserRole.None;

	    [NotMapped]
        [IgnoreMember]
        public bool IsAdmin => UserRole == EUserRole.Administrator;

        [NotMapped]
        [IgnoreMember]
        public bool IsManager => UserRole == EUserRole.Manager;

        [NotMapped]
        [IgnoreMember]
        public bool IsUser => UserRole == EUserRole.User;

        [NotMapped]
        [IgnoreMember]
        public bool IsViewer => UserRole == EUserRole.Viewer;

        [Key(0)]
		public bool IsInvited { get; set; }

        [Key(1)]
        public bool IsRegistered { get; set; }

        [Key(2)]
        public bool IsEnabled { get; set; }

        [Key(3)]
        public string FirstName { get; set; }

        [Key(4)]
        public string LastName { get; set; }

        [Key(5)]
        public bool Terms { get; set; }

        [Key(6)]
        public bool Subscription { get; set; }

        [Key(7)]
        public int InviteQuota { get; set; }

        [Key(8)]
        public int HubQuota { get; set; }

        [Key(9)]
        public string PrivateKey { get; set; }

        [Key(10)]
        public string CertificateChain { get; set; }

        [Key(11)]
        public DateTime? CertificateExpiry { get; set; }
        

        public bool CanLogin()
		{
			return EmailConfirmed && IsInvited && IsEnabled;
		}
    }
}
