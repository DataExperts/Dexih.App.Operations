using System.Collections;
using System.Collections.Generic;

namespace dexih.repository
{
    public class DexihDashboard: DexihHubNamedEntity
    {
        public DexihDashboard()
        {
            DexihDashboardItems = new HashSet<DexihDashboardItem>();
            Parameters = new HashSet<DexihDashboardParameter>();
        }

        public bool IsShared { get; set; }
        public int MinRows { get; set; }
        public int MinCols { get; set; }
        public int MaxRows { get; set; }
        public int MaxCols { get; set; }
        
        public bool AutoRefresh { get; set; }
        
        public ICollection<DexihDashboardItem>  DexihDashboardItems { get; set; }
        
        public ICollection<DexihDashboardParameter> Parameters { get; set; }
    }
}