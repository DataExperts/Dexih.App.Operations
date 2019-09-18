using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkStepParameter: InputParameterBase
    {
        public DexihDatalinkStepParameter()
        {
        }

        [Key(8)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatalinkStep DatalinkStep { get; set; }

    }
}