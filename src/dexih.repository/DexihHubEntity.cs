using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    // [Union(0, typeof(DexihRemoteAgentHub))]
    // [Union(1, typeof(DexihHubNamedEntity))]
    public class DexihHubEntity : DexihBaseEntity
    {
        [DataMember(Order = 3)]
        [JsonIgnore, CopyIgnore, IgnoreDataMember, NotMapped]
        public long HubKey { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember, NotMapped]
        public DexihHub Hub { get; set; }
        
    }
}
