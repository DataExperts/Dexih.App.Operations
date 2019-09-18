using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkParameter: InputParameterBase
    {
        public DexihDatalinkParameter()
        {
        }

        [Key(8)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatalink Datalink { get; set; }

    }
}