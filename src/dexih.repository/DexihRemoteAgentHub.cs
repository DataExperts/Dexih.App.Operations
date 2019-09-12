using System;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihRemoteAgentHub : DexihHubEntity
    {

        [ProtoMember(1)]
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentHubKey { get; set; }

        [ProtoMember(2)]
        public long RemoteAgentKey { get; set; }

        [ProtoMember(3)]
        public bool IsDefault { get; set; }

        [ProtoMember(4)]
        public bool IsAuthorized { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihRemoteAgent RemoteAgent { get; set; }

    }
}
