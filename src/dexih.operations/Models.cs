using System;
using System.Collections.Generic;
using System.Security.Claims;
using dexih.repository;
using MessagePack;
using Microsoft.AspNetCore.Identity;

namespace dexih.operations
{
    [MessagePackObject]
    public class HubUser
    {
        [Key(0)]
        public string FirstName { get; set; }
        
        [Key(1)]
        public string LastName { get; set; }
        
        [Key(2)]
        public string Email { get; set; }
        
        [Key(3)]
        public string Id { get; set; }
        
        [Key(4)]
        public EPermission Permission { get; set; }
    }
    
    /// <summary>
    /// Similar to ApplicationUser class, but excludes password hash and other information we don't want
    /// included for security purposes.
    /// </summary>
    [MessagePackObject]
    public class UserModel
    {
        [Key(0)]
        public string Email { get; set; }
        
        [Key(1)]
        public string UserName { get; set; }
        
        [Key(2)]
        public bool EmailConfirmed { get; set; }

        [Key(3)]
        public int AccessFailedCount { get; set; }
        
        [Key(4)]
        public string Id { get; set; }
        
        [Key(5)]
        public bool LockoutEnabled { get; set; }
        
        [Key(6)]
        public DateTimeOffset? LockoutEnd { get; set; }

        [Key(7)]
        public ICollection<UserLoginInfo> Logins { get; set; }
        
        [Key(8)]
        public ICollection<string> Roles { get; set; }
        
        [Key(9)]
        public ICollection<Claim> Claims { get; set; }

        [Key(10)]
        public bool TwoFactorEnabled { get; set; }
        
        [Key(11)]
        public string PhoneNumber { get; set; }
        
        [Key(12)]
        public bool PhoneNumberConfirmed { get; set; }

        [Key(13)]
        public string FirstName { get; set; }
        
        [Key(14)]
        public string LastName { get; set; }
        
        [Key(15)]
        public bool Terms { get; set; }
        
        [Key(16)]
        public bool Subscription { get; set; }

        [Key(17)]
        public int InviteQuota { get; set; }
        
        [Key(18)]
        public int HubQuota { get; set; }

        [Key(19)]
        public bool IsRegistered { get; set; }
        
        [Key(20)]
        
        public bool IsInvited { get; set; }
        
        [Key(21)]
        public bool IsEnabled { get; set; }
    }

    [MessagePackObject]
    public class ImportAction
    {
        [Key(0)]
        public ESharedObjectType ObjectType { get; set; }

        [Key(1)]
        public EImportAction Action { get; set; }
    }
    
}