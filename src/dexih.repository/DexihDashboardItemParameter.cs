using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDashboardItemParameter: InputParameterBase
    {
        public DexihDashboardItemParameter()
        {
        }

        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DashboardItemKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDashboardItem DashboardItem { get; set; }

    }
}