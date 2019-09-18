using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using MessagePack;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;
using dexih.functions;

namespace dexih.repository
{
    [MessagePackObject]
    [ProtoInherit(100000)]
    [MessagePack.Union(0, typeof(DexihApi))]
    [MessagePack.Union(1, typeof(DexihColumnBase))]
    [MessagePack.Union(2, typeof(DexihColumnValidation))]
    [MessagePack.Union(3, typeof(DexihConnection))]
    [MessagePack.Union(4, typeof(DexihCustomFunction))]
    [MessagePack.Union(5, typeof(DexihDashboard))]
    [MessagePack.Union(6, typeof(DexihDashboardItem))]
    [MessagePack.Union(7, typeof(DexihDatajob))]
    [MessagePack.Union(8, typeof(DexihDatalink))]
    [MessagePack.Union(9, typeof(DexihDatalinkDependency))]
    [MessagePack.Union(10, typeof(DexihDatalinkProfile))]
    [MessagePack.Union(11, typeof(DexihDatalinkStep))]
    [MessagePack.Union(12, typeof(DexihDatalinkTable))]
    [MessagePack.Union(13, typeof(DexihDatalinkTarget))]
    [MessagePack.Union(14, typeof(DexihDatalinkTest))]
    [MessagePack.Union(15, typeof(DexihDatalinkTestStep))]
    [MessagePack.Union(16, typeof(DexihDatalinkTestTable))]
    [MessagePack.Union(17, typeof(DexihDatalinkTransform))]
    [MessagePack.Union(18, typeof(DexihDatalinkTransformItem))]
    [MessagePack.Union(19, typeof(DexihFileFormat))]
    [MessagePack.Union(20, typeof(DexihHubVariable))]
    [MessagePack.Union(21, typeof(InputParameterBase))]
    [MessagePack.Union(22, typeof(DexihParameterBase))]
    [MessagePack.Union(24, typeof(DexihTable))]
    [MessagePack.Union(25, typeof(DexihTrigger))]
    [MessagePack.Union(26, typeof(DexihView))]
    public class DexihHubNamedEntity : DexihHubEntity
    {
        [Key(4)]
        [CopyCollectionKey((long)0, true)]
        public long Key { get; set; }

        [Key(5)]
        [NotMapped]
        public string Name { get; set; }

        [Key(6)]
        [NotMapped]
        public string Description { get; set; }

        [JsonIgnore, CopyIgnore, NotMapped, IgnoreMember]
        public virtual long ParentKey => 0;
    }
}
