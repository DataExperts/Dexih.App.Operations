using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public partial class DexihDatalinkDependency : DexihHubBaseEntity
    {

        [CopyCollectionKey((long)0, true)]
        public long DatalinkDependencyKey { get; set; }

		[CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        public long DependentDatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore]
         public virtual DexihDatalinkStep DatalinkStep { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkStep DependentDatalinkStep { get; set; }


    }
}
