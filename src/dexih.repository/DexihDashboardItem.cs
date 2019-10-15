using System.Collections.Generic;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDashboardItem: DexihHubNamedEntity
    {
        public DexihDashboardItem()
        {
            Parameters = new HashSet<DexihDashboardItemParameter>();
        }

        [Key(7)]
        public int Cols { get; set; }

        [Key(8)]
        public int Rows { get; set; }

        [Key(9)]
        public int X { get; set; }

        [Key(10)]
        public int Y { get; set; }

        [Key(11)]
        public bool Header { get; set; }

        [Key(12)]
        public bool Scrollable { get; set; }

        [Key(13)]
        public long ViewKey { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihView View { get; set; }

        [Key(14)]
        [CopyParentCollectionKey(nameof(Key))]
        public long? DashboardKey { get; set; }

        [Key(15)]
        public ICollection<DexihDashboardItemParameter> Parameters { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDashboard Dashboard { get; set; }

    }
}