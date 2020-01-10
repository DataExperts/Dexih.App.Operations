using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihViewParameter: InputParameterBase
    {
        [DataMember(Order = 11)]
        [CopyParentCollectionKey]
        public long ViewKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihView View { get; set; }

    }
}