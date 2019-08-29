using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDashboardParameter: InputParameterBase
    {
        public DexihDashboardParameter()
        {
        }
        
        [CopyParentCollectionKey]
        public long DashboardKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDashboard Dashboard { get; set; }

    }
}