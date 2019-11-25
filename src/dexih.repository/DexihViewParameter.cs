using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihViewParameter: InputParameterBase
    {
        [Key(9)]
        [CopyParentCollectionKey]
        public long ViewKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihView View { get; set; }

    }
}