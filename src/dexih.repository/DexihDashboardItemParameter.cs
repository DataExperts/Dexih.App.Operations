using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDashboardItemParameter: InputParameterBase
    {
        public DexihDashboardItemParameter()
        {
        }
        
        [CopyParentCollectionKey]
        public long DashboardItemKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDashboardItem DashboardItem { get; set; }

    }
}