using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkStepParameter: InputParameterBase
    {
        [DataMember(Order = 11)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalinkStep DatalinkStep { get; set; }

    }
}