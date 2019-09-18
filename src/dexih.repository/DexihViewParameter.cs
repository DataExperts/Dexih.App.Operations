using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihViewParameter: InputParameterBase
    {
        public DexihViewParameter()
        {
        }

        [Key(8)]
        [CopyParentCollectionKey]
        public long ViewKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihView View { get; set; }

    }
}