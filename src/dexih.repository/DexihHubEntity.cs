using System;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    [ProtoInclude(100, typeof(DexihRemoteAgentHub))]
    [ProtoInclude(200, typeof(DexihHubNamedEntity))]
    public class DexihHubEntity : DexihBaseEntity
    {
        [ProtoMember(1)]
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
        
    }
    
  
}
