using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    public class DexihRemoteAgentHub : DexihHubEntity
    {

        [DataMember(Order = 4)]
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentHubKey { get; set; }

        [DataMember(Order = 5)]
        public long RemoteAgentKey { get; set; }

        [DataMember(Order = 6)]
        public bool IsDefault { get; set; }

        [DataMember(Order = 7)]
        public bool IsAuthorized { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public virtual DexihRemoteAgent RemoteAgent { get; set; }

    }
}
