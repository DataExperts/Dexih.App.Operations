using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatajobParameter: InputParameterBase
    {
        public DexihDatajobParameter()
        {
        }
        
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatajob Datajob { get; set; }

    }
}