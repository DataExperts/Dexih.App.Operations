using MessagePack;
using System.Collections.Generic;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDashboard: DexihHubNamedEntity
    {
        public DexihDashboard()
        {
            DexihDashboardItems = new HashSet<DexihDashboardItem>();
            Parameters = new HashSet<DexihDashboardParameter>();
        }

        [Key(7)]
        public bool IsShared { get; set; }

        [Key(8)]
        public int MinRows { get; set; }

        [Key(9)]
        public int MinCols { get; set; }

        [Key(10)]
        public int MaxRows { get; set; }

        [Key(11)]
        public int MaxCols { get; set; }

        [Key(12)]
        public bool AutoRefresh { get; set; }

        [Key(13)]
        public ICollection<DexihDashboardItem>  DexihDashboardItems { get; set; }

        [Key(14)]
        public ICollection<DexihDashboardParameter> Parameters { get; set; }
        
        public override void ResetKeys()
        {
            Key = 0;

            foreach (var item in DexihDashboardItems)
            {
                item.ResetKeys();
            }
            
            foreach (var parameter in Parameters)
            {
                parameter.ResetKeys();
            }
        }
    }
}