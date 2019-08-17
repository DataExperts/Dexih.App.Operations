using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihViewParameter: DexihHubNamedEntity
    {
        public DexihViewParameter()
        {
        }
        
        public string Value { get; set; }
        
        [CopyParentCollectionKey]
        public long ViewKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihView View { get; set; }

    }
}