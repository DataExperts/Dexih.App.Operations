using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkParameter: InputParameterBase
    {
        [Key(8)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatalink Datalink { get; set; }

    }
}