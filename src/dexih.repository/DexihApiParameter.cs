using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihApiParameter: InputParameterBase
    {
        [Key(9)]
        [CopyParentCollectionKey]
        public long ApiKey { get; set; }

        [Key(10)]
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihApi Api { get; set; }

    }
}