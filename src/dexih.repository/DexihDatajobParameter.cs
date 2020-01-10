using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDatajobParameter: InputParameterBase
    {
        [DataMember(Order = 11)]
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatajob Datajob { get; set; }

    }
}