using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDatajobParameter: InputParameterBase
    {
        public DexihDatajobParameter()
        {
        }

        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatajob Datajob { get; set; }

    }
}