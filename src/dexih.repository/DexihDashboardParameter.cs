using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDashboardParameter: InputParameterBase
    {
        public DexihDashboardParameter()
        {
        }

        [Key(8)]
        [CopyParentCollectionKey]
        public long DashboardKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDashboard Dashboard { get; set; }

    }
}