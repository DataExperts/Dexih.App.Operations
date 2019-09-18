using System;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    [ProtoInherit(1000000)]
    [MessagePack.Union(0, typeof(DexihRemoteAgentHub))]
    [MessagePack.Union(1, typeof(DexihHubNamedEntity))]
    public class DexihHubEntity : DexihBaseEntity
    {
        [Key(3)]
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public long HubKey { get; set; }
        
    }
    
  
}
