using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public partial class DexihRemoteAgentHub : DexihHubEntity
    {

        [Key(4)]
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentHubKey { get; set; }

        [Key(5)]
        public long RemoteAgentKey { get; set; }

        [Key(6)]
        public bool IsDefault { get; set; }

        [Key(7)]
        public bool IsAuthorized { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihRemoteAgent RemoteAgent { get; set; }

    }
}
