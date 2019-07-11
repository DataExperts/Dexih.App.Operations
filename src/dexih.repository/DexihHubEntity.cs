using System;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    [Serializable]
    public class DexihHubEntity : DexihBaseEntity
    {
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
        
    }
    
    [Serializable]
    public class DexihHubNamedEntity: DexihHubEntity
    {
        
        [CopyCollectionKey((long)0, true)]
        public long Key { get; set; }

        [NotMapped]
        public string Name { get; set; }
        
        [NotMapped]
        public string Description { get; set; }

        [JsonIgnore, CopyIgnore, NotMapped]
        public virtual long ParentKey => 0;


    }
}
