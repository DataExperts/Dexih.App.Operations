using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkTarget: DexihHubNamedEntity
    {

        [DataMember(Order = 7)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [DataMember(Order = 8)]
        public long? NodeDatalinkColumnKey { get; set; }

        [DataMember(Order = 9)]
        public DexihDatalinkColumn NodeDatalinkColumn { get; set; }

        [DataMember(Order = 10)]
        public int Position { get; set; }

        [DataMember(Order = 11)]
        public long TableKey { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
         public DexihTable Table { get; set; }

        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalink Datalink { get; set; }

    }
}