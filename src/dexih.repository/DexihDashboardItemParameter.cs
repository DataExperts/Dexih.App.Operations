using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDashboardItemParameter: InputParameterBase
    {
        [Key(9)]
        [CopyParentCollectionKey]
        public long DashboardItemKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDashboardItem DashboardItem { get; set; }

    }
}