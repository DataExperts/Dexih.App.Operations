using System;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    [Serializable]
    public class DexihDatalinkStepColumn : DexihColumnBase
    {

        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }
        
        
        [JsonIgnore, CopyIgnore]
        public DexihDatalinkStep DatalinkStep { get; set; }

        
    }
}