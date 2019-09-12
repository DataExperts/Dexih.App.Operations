using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDatalinkParameter: InputParameterBase
    {
        public DexihDatalinkParameter()
        {
        }

        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalink Datalink { get; set; }

    }
}