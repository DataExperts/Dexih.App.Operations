﻿using System.Collections.Generic;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public partial class DexihDatalinkStep : DexihHubNamedEntity
    {
        public DexihDatalinkStep()
        {
            DexihDatalinkDependencies = new HashSet<DexihDatalinkDependency>();
            DexihDatalinkDependentSteps = new HashSet<DexihDatalinkDependency>();
            DexihDatalinkStepColumns = new HashSet<DexihDatalinkStepColumn>();
            Parameters = new HashSet<DexihDatalinkStepParameter>();
        }

        [Key(7)]
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [Key(8)]
        public long? DatalinkKey { get; set; }


        [Key(9)]
        public ICollection<DexihDatalinkStepColumn> DexihDatalinkStepColumns { get; set; }

        [Key(10)]
        public ICollection<DexihDatalinkDependency> DexihDatalinkDependencies { get; set; }

        [Key(11)]
        public ICollection<DexihDatalinkStepParameter> Parameters { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public ICollection<DexihDatalinkDependency> DexihDatalinkDependentSteps { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatajob Datajob { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatalink Datalink { get; set; }

    }
}
