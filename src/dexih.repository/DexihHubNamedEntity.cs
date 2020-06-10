using Dexih.Utils.CopyProperties;


using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;

namespace dexih.repository
{
    [DataContract]
    // [Union(0, typeof(DexihApi))]
    // [Union(1, typeof(DexihColumnBase))]
    // [Union(2, typeof(DexihColumnValidation))]
    // [Union(3, typeof(DexihConnection))]
    // [Union(4, typeof(DexihCustomFunction))]
    // [Union(5, typeof(DexihDashboard))]
    // [Union(6, typeof(DexihDashboardItem))]
    // [Union(7, typeof(DexihDatajob))]
    // [Union(8, typeof(DexihDatalink))]
    // [Union(9, typeof(DexihDatalinkDependency))]
    // [Union(10, typeof(DexihDatalinkProfile))]
    // [Union(11, typeof(DexihDatalinkStep))]
    // [Union(12, typeof(DexihDatalinkTable))]
    // [Union(13, typeof(DexihDatalinkTarget))]
    // [Union(14, typeof(DexihDatalinkTest))]
    // [Union(15, typeof(DexihDatalinkTestStep))]
    // [Union(16, typeof(DexihDatalinkTestTable))]
    // [Union(17, typeof(DexihDatalinkTransform))]
    // [Union(18, typeof(DexihDatalinkTransformItem))]
    // [Union(19, typeof(DexihFileFormat))]
    // [Union(20, typeof(DexihHubVariable))]
    // [Union(21, typeof(InputParameterBase))]
    // [Union(22, typeof(DexihParameterBase))]
    // [Union(24, typeof(DexihTable))]
    // [Union(25, typeof(DexihTrigger))]
    // [Union(26, typeof(DexihView))]
    public class DexihHubNamedEntity : DexihHubKeyEntity
    {
        [DataMember(Order = 5)]
        [NotMapped]
        public string Name { get; set; }

        [DataMember(Order = 6)]
        [NotMapped]
        public string Description { get; set; }

        [JsonIgnore, CopyIgnore, NotMapped, IgnoreDataMember]
        public virtual long ParentKey => 0;
        
    }
}
