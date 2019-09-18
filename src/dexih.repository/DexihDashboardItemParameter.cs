using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDashboardItemParameter: InputParameterBase
    {
        public DexihDashboardItemParameter()
        {
        }

        [Key(8)]
        [CopyParentCollectionKey]
        public long DashboardItemKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDashboardItem DashboardItem { get; set; }

    }
}