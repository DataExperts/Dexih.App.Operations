using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDashboardParameter: DexihHubNamedEntity
    {
        public DexihDashboardParameter()
        {
        }
        
        public string Value { get; set; }
        
        [CopyParentCollectionKey]
        public long DashboardKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDashboard Dashboard { get; set; }

    }
}