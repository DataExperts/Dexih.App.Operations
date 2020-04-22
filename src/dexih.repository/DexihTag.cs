using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [DataContract]
    public class DexihTag: DexihHubNamedEntity
    {
        public DexihTag()
        {
            DexihTagObjects = new HashSet<DexihTagObject>();
        }
        
        [DataMember(Order = 5)]
        public string Color { get; set; }

        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public ICollection<DexihTagObject> DexihTagObjects { get; set; }
            
    }
}