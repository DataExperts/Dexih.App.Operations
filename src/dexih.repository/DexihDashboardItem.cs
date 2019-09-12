using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDashboardItem: DexihHubNamedEntity
    {
        public DexihDashboardItem()
        {
            Parameters = new HashSet<DexihDashboardItemParameter>();
        }

        [ProtoMember(1)]
        public int Cols { get; set; }

        [ProtoMember(2)]
        public int Rows { get; set; }

        [ProtoMember(3)]
        public int X { get; set; }

        [ProtoMember(4)]
        public int Y { get; set; }

        [ProtoMember(5)]
        public bool Header { get; set; }

        [ProtoMember(6)]
        public bool Scrollable { get; set; }

        [ProtoMember(7)]
        public long ViewKey { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihView View { get; set; }

        [ProtoMember(8)]
        [CopyParentCollectionKey(nameof(Key))]
        public long? DashboardKey { get; set; }

        [ProtoMember(9)]
        public ICollection<DexihDashboardItemParameter> Parameters { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihDashboard Dashboard { get; set; }

    }
}