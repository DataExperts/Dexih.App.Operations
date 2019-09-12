using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDashboardParameter: InputParameterBase
    {
        public DexihDashboardParameter()
        {
        }

        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DashboardKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDashboard Dashboard { get; set; }

    }
}