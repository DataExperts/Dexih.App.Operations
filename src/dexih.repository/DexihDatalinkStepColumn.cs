using System;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDatalinkStepColumn : DexihColumnBase
    {

        [ProtoMember(2)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }
        
        
        [JsonIgnore, CopyIgnore]
        public DexihDatalinkStep DatalinkStep { get; set; }

        
    }
}