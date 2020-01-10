using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkProfile : DexihHubNamedEntity
    {
        [DataMember(Order = 7)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [DataMember(Order = 8)]
        public string FunctionClassName { get; set; }

        [DataMember(Order = 9)]
        public string FunctionAssemblyName { get; set; }

        [DataMember(Order = 10)]
        public string FunctionMethodName { get; set; }

        [DataMember(Order = 11)]
        public bool DetailedResults { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public virtual DexihDatalink Datalink { get; set; }
        
        public override void ResetKeys()
        {
            Key = 0;
        }
    }
}
