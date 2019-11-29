using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatajobParameter: InputParameterBase
    {
        [Key(11)]
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatajob Datajob { get; set; }

    }
}