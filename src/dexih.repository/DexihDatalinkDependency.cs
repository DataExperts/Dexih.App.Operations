using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkDependency : DexihHubKeyEntity
    {
        [DataMember(Order = 7)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        [DataMember(Order = 8)]
        public long DependentDatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
         public virtual DexihDatalinkStep DatalinkStep { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public virtual DexihDatalinkStep DependentDatalinkStep { get; set; }


    }
}
