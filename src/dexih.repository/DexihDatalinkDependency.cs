using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public partial class DexihDatalinkDependency : DexihHubNamedEntity
    {
        [Key(7)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        [Key(8)]
        public long DependentDatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
         public virtual DexihDatalinkStep DatalinkStep { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihDatalinkStep DependentDatalinkStep { get; set; }


    }
}
