using System;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [Serializable]
    public partial class DexihRemoteAgentHub : DexihHubEntity
    {
        
        [CopyCollectionKey((long)0, true)]
        public long RemoteAgentHubKey { get; set; }

        // [CopyParentCollectionKey]
        public long RemoteAgentKey { get; set; }
        
        
        public bool IsDefault { get; set; }
        public bool IsAuthorized { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihRemoteAgent RemoteAgent { get; set; }

    }
}
