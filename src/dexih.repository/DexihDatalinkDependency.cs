using System.Collections.Generic;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public partial class DexihDatalinkDependency : DexihBaseEntity
    {
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

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
