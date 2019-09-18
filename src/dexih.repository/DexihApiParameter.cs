using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihApiParameter: InputParameterBase
    {
        public DexihApiParameter()
        {
        }

        [Key(8)]
        [CopyParentCollectionKey]
        public long ApiKey { get; set; }

        [Key(9)]
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihApi Api { get; set; }

    }
}