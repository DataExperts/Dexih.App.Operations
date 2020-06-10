using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkTarget: DexihHubKeyEntity
    {

        [DataMember(Order = 5)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [DataMember(Order = 6)]
        public long? NodeDatalinkColumnKey { get; set; }

        [DataMember(Order = 7)]
        public DexihDatalinkColumn NodeDatalinkColumn { get; set; }

        [DataMember(Order = 8)]
        public int Position { get; set; }

        [DataMember(Order = 9)]
        public long TableKey { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
         public DexihTable Table { get; set; }

        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalink Datalink { get; set; }

    }
}