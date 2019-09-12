using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace dexih.repository
{
    [ProtoContract]
    [ProtoInclude(100, typeof(DexihApi))]
    [ProtoInclude(300, typeof(DexihColumnBase))]
    [ProtoInclude(400, typeof(DexihColumnValidation))]
    [ProtoInclude(500, typeof(DexihConnection))]
    [ProtoInclude(600, typeof(DexihCustomFunction))]
    [ProtoInclude(800, typeof(DexihDashboard))]
    [ProtoInclude(900, typeof(DexihDashboardItem))]
    [ProtoInclude(1200, typeof(DexihDatajob))]
    [ProtoInclude(1400, typeof(DexihDatalink))]
    [ProtoInclude(1600, typeof(DexihDatalinkDependency))]
    [ProtoInclude(1800, typeof(DexihDatalinkProfile))]
    [ProtoInclude(1900, typeof(DexihDatalinkStep))]
    [ProtoInclude(2200, typeof(DexihDatalinkTable))]
    [ProtoInclude(2300, typeof(DexihDatalinkTarget))]
    [ProtoInclude(2400, typeof(DexihDatalinkTest))]
    [ProtoInclude(2500, typeof(DexihDatalinkTestStep))]
    [ProtoInclude(2600, typeof(DexihDatalinkTestTable))]
    [ProtoInclude(2700, typeof(DexihDatalinkTransform))]
    [ProtoInclude(2800, typeof(DexihDatalinkTransformItem))]
    [ProtoInclude(2900, typeof(DexihFileFormat))]
    [ProtoInclude(3600, typeof(DexihHubVariable))]
    [ProtoInclude(3700, typeof(InputParameterBase))]
    [ProtoInclude(3800, typeof(DexihParameterBase))]
    [ProtoInclude(4000, typeof(DexihRemoteAgentHub))]
    [ProtoInclude(4100, typeof(DexihTable))]
    [ProtoInclude(4300, typeof(DexihTrigger))]
    [ProtoInclude(4400, typeof(DexihView))]
    public class DexihHubNamedEntity : DexihHubEntity
    {
        [ProtoMember(1)]
        [CopyCollectionKey((long)0, true)]
        public long Key { get; set; }

        [ProtoMember(2)]
        [NotMapped]
        public string Name { get; set; }

        [ProtoMember(3)]
        [NotMapped]
        public string Description { get; set; }

        [JsonIgnore, CopyIgnore, NotMapped]
        public virtual long ParentKey => 0;
    }
}
