using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihRemoteAgent : DexihBaseEntity
    {        
        public DexihRemoteAgent()
        {
            DexihremoteAgentHubs = new HashSet<DexihRemoteAgentHub>();
        }
        
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentKey { get; set; }

        public string Name { get; set; }
        public string UserId { get; set; }

        public bool RestrictIp { get; set; }

        public bool AllowExternalConnect { get; set; }
        [NotMapped]
        public string[] IpAddresses { get; set; }

        [JsonIgnore, CopyIgnore]
        public string IpAddressesString
        {
            get => IpAddresses == null ? null : string.Join(",", IpAddresses);
            set => IpAddresses = value?.Split(',').ToArray();
        }
        public string RemoteAgentId { get; set; }
        
        public string HashedToken { get; set; }

        public string LastLoginIpAddress { get; set; }
        public DateTime? LastLoginDateTime { get; set; } 
        
        [NotMapped]
        public DexihActiveAgent[] ActiveAgents { get; set; }
        
        public virtual ICollection<DexihRemoteAgentHub> DexihremoteAgentHubs { get; set; }
    }
}
