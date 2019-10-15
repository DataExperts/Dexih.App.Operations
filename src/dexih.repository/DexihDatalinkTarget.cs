using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkTarget: DexihHubNamedEntity
    {

        [Key(7)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [Key(8)]
        public long? NodeDatalinkColumnKey { get; set; }

        [Key(9)]
        public DexihDatalinkColumn NodeDatalinkColumn { get; set; }

        [Key(10)]
        public int Position { get; set; }

        [Key(11)]
        public long TableKey { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
         public DexihTable Table { get; set; }

        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatalink Datalink { get; set; }
    }
}