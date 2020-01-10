using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkStepColumn : DexihColumnBase
    {

        [DataMember(Order = 24)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }
        
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalinkStep DatalinkStep { get; set; }

        
    }
}