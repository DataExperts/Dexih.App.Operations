using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatalinkStepParameter: InputParameterBase
    {
        public DexihDatalinkStepParameter()
        {
        }
        
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalinkStep DatalinkStep { get; set; }

    }
}