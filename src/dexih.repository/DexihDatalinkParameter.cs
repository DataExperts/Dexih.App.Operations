using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatalinkParameter: InputParameterBase
    {
        public DexihDatalinkParameter()
        {
        }
        
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalink Datalink { get; set; }

    }
}