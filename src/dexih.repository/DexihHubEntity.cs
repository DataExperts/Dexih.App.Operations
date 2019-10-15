using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    [Union(0, typeof(DexihRemoteAgentHub))]
    [Union(1, typeof(DexihHubNamedEntity))]
    public class DexihHubEntity : DexihBaseEntity
    {
        [Key(3)]
        [JsonIgnore, CopyIgnore, IgnoreMember, NotMapped]
        public long HubKey { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreMember, NotMapped]
        public DexihHub Hub { get; set; }
        
    }
    
  
}
