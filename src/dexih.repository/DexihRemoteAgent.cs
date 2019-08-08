using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [Serializable]
    public partial class DexihRemoteAgent : DexihBaseEntity
    {        
        public DexihRemoteAgent()
        {
            DexihRemoteAgentHubs = new HashSet<DexihRemoteAgentHub>();
        }
        
        
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentKey { get; set; }

        public string Name { get; set; }
        public string UserId { get; set; }

        public bool RestrictIp { get; set; }

        public bool AllowExternalConnect { get; set; }
        public List<string> IpAddresses { get; set; }

        public string RemoteAgentId { get; set; }
        
        public string HashedToken { get; set; }

        public string LastLoginIpAddress { get; set; }
        public DateTime? LastLoginDateTime { get; set; } 
        
        [NotMapped]
        public DexihActiveAgent[] ActiveAgents { get; set; }
        
        public virtual ICollection<DexihRemoteAgentHub> DexihRemoteAgentHubs { get; set; }
    }
}
