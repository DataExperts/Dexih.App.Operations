using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihViewParameter: InputParameterBase
    {
        public DexihViewParameter()
        {
        }

        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long ViewKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihView View { get; set; }

    }
}