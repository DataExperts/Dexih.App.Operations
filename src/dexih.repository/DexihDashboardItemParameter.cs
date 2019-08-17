using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDashboardItemParameter: DexihHubNamedEntity
    {
        public DexihDashboardItemParameter()
        {
        }
        
        public string Value { get; set; }
        
        [CopyParentCollectionKey]
        public long DashboardItemKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDashboardItem DashboardItem { get; set; }

    }
}