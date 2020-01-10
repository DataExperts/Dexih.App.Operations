using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDashboardItemParameter: InputParameterBase
    {
        [DataMember(Order = 11)]
        [CopyParentCollectionKey]
        public long DashboardItemKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDashboardItem DashboardItem { get; set; }

    }
}