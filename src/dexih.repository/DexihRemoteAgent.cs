using System;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihRemoteAgent : DexihBaseEntity
    {
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
        public string Name { get; set; }
        
        public bool RestrictIp { get; set; }

        [NotMapped]
        public string UserId { get; set; }

        [NotMapped]
        public string[] IpAddresses { get; set; }

        [JsonIgnore, CopyIgnore]
        public string IpAddressesString
        {
            get => IpAddresses == null ? null : string.Join(",", IpAddresses);
            set => IpAddresses = value?.Split(',').ToArray();
        }
        public string RemoteAgentId { get; set; }

        public bool IsDefault { get; set; }
        public bool AllowExternalConnect { get; set; }

        public string LastLoginIpAddress { get; set; }
        public DateTime? LastLoginDate { get; set; } 

        [NotMapped]
        public bool IsRunning { get; set; }
        
        [NotMapped]
        public bool IsActive => IsRunning && IsAuthorized && (!RestrictIp || (IpAddresses.Contains(IpAddress)));

        [NotMapped]
        public string Status {
            get
            {
                if (!IsRunning) return "Offline";
                if (!IsAuthorized) return "Unauthorized";
                if (IsActive) return "Ready";
                if (RestrictIp && !IpAddresses.Contains((IpAddress))) return "Invalid IpAddress";
                return "Unknown";
            } 
        }
        
        [NotMapped]
        public bool IsAuthorized { get; set; }
        
        [NotMapped]
        public string IpAddress { get; set; }
        
        [NotMapped]
        public string InstanceId { get; set; }
        
    }
}
