using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihDatalinkStep : DexihHubNamedEntity
    {
        public DexihDatalinkStep()
        {
            DexihDatalinkDependencies = new HashSet<DexihDatalinkDependency>();
            DexihDatalinkDependentSteps = new HashSet<DexihDatalinkDependency>();
            DexihDatalinkStepColumns = new HashSet<DexihDatalinkStepColumn>();
            Parameters = new HashSet<DexihDatalinkStepParameter>();
        }

        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [ProtoMember(2)]
        public long? DatalinkKey { get; set; }


        [ProtoMember(3)]
        public ICollection<DexihDatalinkStepColumn> DexihDatalinkStepColumns { get; set; }

        [ProtoMember(4)]
        public ICollection<DexihDatalinkDependency> DexihDatalinkDependencies { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkDependency> DexihDatalinkDependentSteps { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatajob Datajob { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public DexihDatalink Datalink { get; set; }

        [ProtoMember(5)]
        public ICollection<DexihDatalinkStepParameter> Parameters { get; set; }
    }
}
