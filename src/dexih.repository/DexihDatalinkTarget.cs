using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatalinkTarget: DexihBaseEntity
    {
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        [CopyCollectionKey((long)0, true)]
        public long DatalinkTargetKey { get; set; }

        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }
        
        public long? NodeDatalinkColumnKey { get; set; }
        
        public DexihDatalinkColumn NodeDatalinkColumn { get; set; }
        
        public long TableKey { get; set; }
        
        [JsonIgnore, CopyIgnore]
         public DexihTable Table { get; set; }

        
        [JsonIgnore, CopyIgnore]
        public DexihDatalink Datalink { get; set; }
    }
}