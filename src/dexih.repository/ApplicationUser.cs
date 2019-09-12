using System;
using System.ComponentModel.DataAnnotations.Schema;
using Microsoft.AspNetCore.Identity;
using ProtoBuf;

namespace dexih.repository
{
    // Add profile data for application users by adding properties to the ApplicationUser class
    [ProtoContract]
    public class ApplicationUser : IdentityUser
    {
    
	    public enum EUserRole
	    {
            Administrator = 1, Manager, User, Viewer, None
	    }

	    [NotMapped]
        [ProtoIgnore]
        public EUserRole UserRole { get; set; } = EUserRole.None;

	    [NotMapped]
        [ProtoIgnore]
        public bool IsAdmin => UserRole == EUserRole.Administrator;

        [NotMapped]
        [ProtoIgnore]
        public bool IsManager => UserRole == EUserRole.Manager;

        [NotMapped]
        [ProtoIgnore]
        public bool IsUser => UserRole == EUserRole.User;

        [NotMapped]
        [ProtoIgnore]
        public bool IsViewer => UserRole == EUserRole.Viewer;

        [ProtoMember(1)]
		public bool IsInvited { get; set; }

        [ProtoMember(2)]
        public bool IsRegistered { get; set; }

        [ProtoMember(3)]
        public bool IsEnabled { get; set; }

        [ProtoMember(4)]
        public string FirstName { get; set; }

        [ProtoMember(5)]
        public string LastName { get; set; }

        [ProtoMember(6)]
        public bool Terms { get; set; }

        [ProtoMember(7)]
        public bool Subscription { get; set; }

        [ProtoMember(8)]
        public int InviteQuota { get; set; }

        [ProtoMember(9)]
        public int HubQuota { get; set; }

        [ProtoMember(10)]
        public string PrivateKey { get; set; }

        [ProtoMember(11)]
        public string CertificateChain { get; set; }

        [ProtoMember(12)]
        public DateTime? CertificateExpiry { get; set; }

		public bool CanLogin()
		{
			return EmailConfirmed && IsInvited && IsEnabled;
		}
    }
}
