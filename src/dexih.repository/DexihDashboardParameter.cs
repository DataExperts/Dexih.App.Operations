using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDashboardParameter: InputParameterBase
    {
        [DataMember(Order = 11)]
        [CopyParentCollectionKey]
        public long DashboardKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDashboard Dashboard { get; set; }

    }
}