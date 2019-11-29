using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihApiParameter: InputParameterBase
    {
        [Key(11)]
        [CopyParentCollectionKey]
        public long ApiKey { get; set; }

        [Key(12)]
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihApi Api { get; set; }

    }
}