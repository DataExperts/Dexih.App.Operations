using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkParameter: InputParameterBase
    {
        [DataMember(Order = 11)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalink Datalink { get; set; }

    }
}