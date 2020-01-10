using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Security.Claims;
using dexih.repository;

using Microsoft.AspNetCore.Identity;

namespace dexih.operations
{
    [DataContract]
    public class HubUser
    {
        [DataMember(Order = 0)]
        public string FirstName { get; set; }
        
        [DataMember(Order = 1)]
        public string LastName { get; set; }
        
        [DataMember(Order = 2)]
        public string Email { get; set; }
        
        [DataMember(Order = 3)]
        public string Id { get; set; }
        
        [DataMember(Order = 4)]
        public EPermission Permission { get; set; }
    }
    
    /// <summary>
    /// Similar to ApplicationUser class, but excludes password hash and other information we don't want
    /// included for security purposes.
    /// </summary>
    [DataContract]
    public class UserModel
    {
        [DataMember(Order = 0)]
        public string Email { get; set; }
        
        [DataMember(Order = 1)]
        public string UserName { get; set; }
        
        [DataMember(Order = 2)]
        public bool EmailConfirmed { get; set; }

        [DataMember(Order = 3)]
        public int AccessFailedCount { get; set; }
        
        [DataMember(Order = 4)]
        public string Id { get; set; }
        
        [DataMember(Order = 5)]
        public bool LockoutEnabled { get; set; }
        
        [DataMember(Order = 6)]
        public DateTimeOffset? LockoutEnd { get; set; }

        [DataMember(Order = 7)]
        public ICollection<UserLoginInfo> Logins { get; set; }
        
        [DataMember(Order = 8)]
        public ICollection<string> Roles { get; set; }
        
        [DataMember(Order = 9)]
        public ICollection<Claim> Claims { get; set; }

        [DataMember(Order = 10)]
        public bool TwoFactorEnabled { get; set; }
        
        [DataMember(Order = 11)]
        public string PhoneNumber { get; set; }
        
        [DataMember(Order = 12)]
        public bool PhoneNumberConfirmed { get; set; }

        [DataMember(Order = 13)]
        public string FirstName { get; set; }
        
        [DataMember(Order = 14)]
        public string LastName { get; set; }
        
        [DataMember(Order = 15)]
        public bool Terms { get; set; }
        
        [DataMember(Order = 16)]
        public bool Subscription { get; set; }

        [DataMember(Order = 17)]
        public int InviteQuota { get; set; }
        
        [DataMember(Order = 18)]
        public int HubQuota { get; set; }

        [DataMember(Order = 19)]
        public bool IsRegistered { get; set; }
        
        [DataMember(Order = 20)]
        
        public bool IsInvited { get; set; }
        
        [DataMember(Order = 21)]
        public bool IsEnabled { get; set; }
    }

    [DataContract]
    public class ImportAction
    {
        [DataMember(Order = 0)]
        public ESharedObjectType ObjectType { get; set; }

        [DataMember(Order = 1)]
        public EImportAction Action { get; set; }
    }
    
}