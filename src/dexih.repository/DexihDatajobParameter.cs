using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatajobParameter: InputParameterBase
    {
        public DexihDatajobParameter()
        {
        }

        [Key(8)]
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatajob Datajob { get; set; }

    }
}