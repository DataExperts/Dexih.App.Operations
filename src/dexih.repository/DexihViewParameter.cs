using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihViewParameter: InputParameterBase
    {
        public DexihViewParameter()
        {
        }
        
        [CopyParentCollectionKey]
        public long ViewKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihView View { get; set; }

    }
}