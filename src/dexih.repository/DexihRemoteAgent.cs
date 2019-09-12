using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihRemoteAgent : DexihBaseEntity
    {        
        public DexihRemoteAgent()
        {
            DexihRemoteAgentHubs = new HashSet<DexihRemoteAgentHub>();
        }
        
        [ProtoMember(1)]
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentKey { get; set; }

        [ProtoMember(3)]
        public string Name { get; set; }

        [ProtoMember(4)]
        public string UserId { get; set; }

        [ProtoMember(5)]
        public bool RestrictIp { get; set; }

        [ProtoMember(6)]
        public bool AllowExternalConnect { get; set; }

        [ProtoMember(7)]
        public List<string> IpAddresses { get; set; }

        [ProtoMember(8)]
        public string RemoteAgentId { get; set; }

        [ProtoMember(9)]
        public string HashedToken { get; set; }

        [ProtoMember(10)]
        public string LastLoginIpAddress { get; set; }

        [ProtoMember(11)]
        public DateTime? LastLoginDateTime { get; set; }

        [ProtoMember(12)]
        [NotMapped]
        public DexihActiveAgent[] ActiveAgents { get; set; }

        [ProtoMember(13)]
        public virtual ICollection<DexihRemoteAgentHub> DexihRemoteAgentHubs { get; set; }
    }
}
