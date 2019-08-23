using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihApiParameter: InputParameterBase
    {
        public DexihApiParameter()
        {
        }
        
        [CopyParentCollectionKey]
        public long ApiKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihApi Api { get; set; }

    }
}