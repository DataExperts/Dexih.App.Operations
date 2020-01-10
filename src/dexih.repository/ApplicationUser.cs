using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using Microsoft.AspNetCore.Identity;


namespace dexih.repository
{
    // Add profile data for application users by adding properties to the ApplicationUser class
    [DataContract]
    public class ApplicationUser : IdentityUser
    {
	    

	    [NotMapped]
        [IgnoreDataMember]
        public EUserRole UserRole { get; set; } = EUserRole.None;

	    [NotMapped]
        [IgnoreDataMember]
        public bool IsAdmin => UserRole == EUserRole.Administrator;

        [NotMapped]
        [IgnoreDataMember]
        public bool IsManager => UserRole == EUserRole.Manager;

        [NotMapped]
        [IgnoreDataMember]
        public bool IsUser => UserRole == EUserRole.User;

        [NotMapped]
        [IgnoreDataMember]
        public bool IsViewer => UserRole == EUserRole.Viewer;

        [DataMember(Order = 0)]
		public bool IsInvited { get; set; }

        [DataMember(Order = 1)]
        public bool IsRegistered { get; set; }

        [DataMember(Order = 2)]
        public bool IsEnabled { get; set; }

        [DataMember(Order = 3)]
        public string FirstName { get; set; }

        [DataMember(Order = 4)]
        public string LastName { get; set; }

        [DataMember(Order = 5)]
        public bool Terms { get; set; }

        [DataMember(Order = 6)]
        public bool Subscription { get; set; }

        [DataMember(Order = 7)]
        public int InviteQuota { get; set; }

        [DataMember(Order = 8)]
        public int HubQuota { get; set; }

        [DataMember(Order = 9)]
        public string PrivateKey { get; set; }

        [DataMember(Order = 10)]
        public string CertificateChain { get; set; }

        [DataMember(Order = 11)]
        public DateTime? CertificateExpiry { get; set; }
        

        public bool CanLogin()
		{
			return EmailConfirmed && IsInvited && IsEnabled;
		}
    }
}
