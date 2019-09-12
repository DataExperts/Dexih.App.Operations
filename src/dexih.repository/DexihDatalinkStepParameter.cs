using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDatalinkStepParameter: InputParameterBase
    {
        public DexihDatalinkStepParameter()
        {
        }

        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalinkStep DatalinkStep { get; set; }

    }
}