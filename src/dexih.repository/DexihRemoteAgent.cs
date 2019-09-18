using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public partial class DexihRemoteAgent : DexihBaseEntity
    {        
        public DexihRemoteAgent()
        {
            DexihRemoteAgentHubs = new HashSet<DexihRemoteAgentHub>();
        }
        
        [Key(3)]
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentKey { get; set; }

        [Key(4)]
        public string Name { get; set; }

        [Key(5)]
        public string UserId { get; set; }

        [Key(6)]
        public bool RestrictIp { get; set; }

        [Key(7)]
        public bool AllowExternalConnect { get; set; }

        [Key(8)]
        public List<string> IpAddresses { get; set; }

        [Key(9)]
        public string RemoteAgentId { get; set; }

        [Key(10)]
        public string HashedToken { get; set; }

        [Key(11)]
        public string LastLoginIpAddress { get; set; }

        [Key(12)]
        public DateTime? LastLoginDateTime { get; set; }

        [Key(13)]
        [NotMapped]
        public DexihActiveAgent[] ActiveAgents { get; set; }

        [Key(14)]
        public virtual ICollection<DexihRemoteAgentHub> DexihRemoteAgentHubs { get; set; }
    }
}
