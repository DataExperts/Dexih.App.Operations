using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDashboardParameter: InputParameterBase
    {
        [Key(9)]
        [CopyParentCollectionKey]
        public long DashboardKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDashboard Dashboard { get; set; }

    }
}