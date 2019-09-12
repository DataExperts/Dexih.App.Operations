using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihApiParameter: InputParameterBase
    {
        public DexihApiParameter()
        {
        }

        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long ApiKey { get; set; }

        [ProtoMember(2)]
        [JsonIgnore, CopyIgnore]
        public DexihApi Api { get; set; }

    }
}