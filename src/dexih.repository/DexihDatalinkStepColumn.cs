using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatalinkStepColumn : DexihColumnBase
    {


        [CopyCollectionKey((long)0)]
        public long DatalinkStepColumnKey { get; set; }

        public long? DatalinkStepKey { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihDatalinkStep DatalinkStep { get; set; }

        
    }
}