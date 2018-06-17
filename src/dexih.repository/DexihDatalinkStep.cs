using System.Collections.Generic;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihDatalinkStep : DexihBaseEntity
    {
        public DexihDatalinkStep()
        {
            DexihDatalinkDependencies = new HashSet<DexihDatalinkDependency>();
            DexihDatalinkDependentSteps = new HashSet<DexihDatalinkDependency>();
        }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        [CopyCollectionKey((long)0, true)]
        public long DatalinkStepKey { get; set; }
		
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }
        
        public long DatalinkKey { get; set; }

        public string Name { get; set; }

        public virtual ICollection<DexihDatalinkDependency> DexihDatalinkDependencies { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalinkDependency> DexihDatalinkDependentSteps { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatajob Datajob { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalink Datalink { get; set; }
    }
}
