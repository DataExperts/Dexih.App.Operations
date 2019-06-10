using System.Collections.Generic;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihDatalinkStep : DexihHubNamedEntity
    {
        public DexihDatalinkStep()
        {
            DexihDatalinkDependencies = new HashSet<DexihDatalinkDependency>();
            DexihDatalinkDependentSteps = new HashSet<DexihDatalinkDependency>();
            DexihDatalinkStepColumns = new HashSet<DexihDatalinkStepColumn>();
        }


		
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }
        
        public long? DatalinkKey { get; set; }

        
        public ICollection<DexihDatalinkStepColumn> DexihDatalinkStepColumns { get; set; }

        public ICollection<DexihDatalinkDependency> DexihDatalinkDependencies { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkDependency> DexihDatalinkDependentSteps { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatajob Datajob { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihDatalink Datalink { get; set; }
    }
}
