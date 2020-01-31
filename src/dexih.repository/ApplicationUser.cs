using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using Dexih.Utils.CopyProperties;
using Microsoft.AspNetCore.Identity;
using Newtonsoft.Json;


namespace dexih.repository
{
    // Add profile data for application users by adding properties to the ApplicationUser class
    [DataContract]
    public class ApplicationUser : IdentityUser
    {
	    [DataMember(Order = 0)]
	    public override string Id { get; set; }
	    [DataMember(Order = 1)]
	    public override string Email { get; set; }
	    [DataMember(Order = 2)]
	    public override string ConcurrencyStamp { get; set; }
	    [DataMember(Order = 3)]
	    public override bool EmailConfirmed { get; set; }
	    [DataMember(Order = 4)]
	    public override bool LockoutEnabled { get; set; }
	    [DataMember(Order = 5)]
	    public override DateTimeOffset? LockoutEnd { get; set; }
	    [DataMember(Order = 6)]
	    public override string NormalizedEmail { get; set; }
	    [DataMember(Order = 7)]
	    public override string PasswordHash { get; set; }
	    [DataMember(Order = 8)]
	    public override string PhoneNumber { get; set; }
	    [DataMember(Order = 9)]
	    public override string SecurityStamp { get; set; }
	    [DataMember(Order = 10)]
	    public override string UserName { get; set; }
	    [DataMember(Order = 11)]
	    public override int AccessFailedCount { get; set; }
	    [DataMember(Order = 12)]
	    public override string NormalizedUserName { get; set; }
	    [DataMember(Order = 13)]
	    public override bool PhoneNumberConfirmed { get; set; }
	    [DataMember(Order = 14)]
	    public override bool TwoFactorEnabled { get; set; }

	    [DataMember(Order = 15)]
		public bool IsInvited { get; set; }

        [DataMember(Order = 16)]
        public bool IsRegistered { get; set; }

        [DataMember(Order = 17)]
        public bool IsEnabled { get; set; }

        [DataMember(Order = 18)]
        public string FirstName { get; set; }

        [DataMember(Order = 19)]
        public string LastName { get; set; }

        [DataMember(Order = 20)]
        public bool Terms { get; set; }

        [DataMember(Order = 21)]
        public bool Subscription { get; set; }

        [DataMember(Order = 22)]
        public int InviteQuota { get; set; }

        [DataMember(Order = 23)]
        public int HubQuota { get; set; }

        [DataMember(Order = 24)]
        public string PrivateKey { get; set; }

        [DataMember(Order = 25)]
        public string CertificateChain { get; set; }

        [DataMember(Order = 26)]
        public DateTime? CertificateExpiry { get; set; }
        
        [DataMember(Order = 26)]
        public bool NotifyPrivateMessage { get; set; }

        [DataMember(Order = 26)]
        public bool NotifySupportMessage { get; set; }
        
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
        
        public bool CanLogin()
		{
			return EmailConfirmed && IsInvited && IsEnabled;
		}
    }
}
