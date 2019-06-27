using System;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    [Serializable]
    public partial class DexihDatalinkDependency : DexihHubNamedEntity
    {


		[CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        public long DependentDatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore]
         public virtual DexihDatalinkStep DatalinkStep { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkStep DependentDatalinkStep { get; set; }


    }
}
