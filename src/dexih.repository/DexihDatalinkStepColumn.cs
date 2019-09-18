using System;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkStepColumn : DexihColumnBase
    {

        [Key(24)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }
        
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatalinkStep DatalinkStep { get; set; }

        
    }
}