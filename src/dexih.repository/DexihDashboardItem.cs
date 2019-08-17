using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDashboardItem: DexihHubNamedEntity
    {
        public DexihDashboardItem()
        {
            Parameters = new HashSet<DexihDashboardItemParameter>();
        }
        public int Cols { get; set; }
        public int Rows { get; set; }
        public int X { get; set; }
        public int Y { get; set; }
        
        public long ViewKey { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihView View { get; set; }
        
        [CopyParentCollectionKey(nameof(Key))]
        public long? DashboardKey { get; set; }
        
        public ICollection<DexihDashboardItemParameter> Parameters { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihDashboard Dashboard { get; set; }

    }
}