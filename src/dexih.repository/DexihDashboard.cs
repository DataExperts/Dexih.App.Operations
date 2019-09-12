using ProtoBuf;
using System.Collections;
using System.Collections.Generic;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDashboard: DexihHubNamedEntity
    {
        public DexihDashboard()
        {
            DexihDashboardItems = new HashSet<DexihDashboardItem>();
            Parameters = new HashSet<DexihDashboardParameter>();
        }

        [ProtoMember(1)]
        public bool IsShared { get; set; }

        [ProtoMember(2)]
        public int MinRows { get; set; }

        [ProtoMember(3)]
        public int MinCols { get; set; }

        [ProtoMember(4)]
        public int MaxRows { get; set; }

        [ProtoMember(5)]
        public int MaxCols { get; set; }

        [ProtoMember(6)]
        public bool AutoRefresh { get; set; }

        [ProtoMember(7)]
        public ICollection<DexihDashboardItem>  DexihDashboardItems { get; set; }

        [ProtoMember(8)]
        public ICollection<DexihDashboardParameter> Parameters { get; set; }
    }
}