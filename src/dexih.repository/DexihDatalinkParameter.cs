using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatalinkParameter: DexihHubNamedEntity
    {
        public DexihDatalinkParameter()
        {
        }
        
        public string Value { get; set; }
        
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalink Datalink { get; set; }

    }
}