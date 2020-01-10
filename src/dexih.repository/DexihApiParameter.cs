using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihApiParameter: InputParameterBase
    {
        [DataMember(Order = 11)]
        [CopyParentCollectionKey]
        public long ApiKey { get; set; }

        [DataMember(Order = 12)]
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihApi Api { get; set; }

    }
}