﻿using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkStep : DexihHubNamedEntity
    {
        public DexihDatalinkStep()
        {
            DexihDatalinkDependencies = new HashSet<DexihDatalinkDependency>();
            DexihDatalinkDependentSteps = new HashSet<DexihDatalinkDependency>();
            DexihDatalinkStepColumns = new HashSet<DexihDatalinkStepColumn>();
            Parameters = new HashSet<DexihDatalinkStepParameter>();
        }

        [DataMember(Order = 7)]
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [DataMember(Order = 8)]
        public long? DatalinkKey { get; set; }
        
        [DataMember(Order = 9)]
        public int Position { get; set; }

        [DataMember(Order = 10)]
        public ICollection<DexihDatalinkStepColumn> DexihDatalinkStepColumns { get; set; }

        [DataMember(Order = 11)]
        public ICollection<DexihDatalinkDependency> DexihDatalinkDependencies { get; set; }

        [DataMember(Order = 12)]
        public ICollection<DexihDatalinkStepParameter> Parameters { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public ICollection<DexihDatalinkDependency> DexihDatalinkDependentSteps { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatajob Datajob { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalink Datalink { get; set; }
        
        public override void ResetKeys()
        {
            base.ResetKeys();
            
            foreach (var parameter in Parameters)
            {
                parameter.ResetKeys();
            }
            
            foreach(var dep in DexihDatalinkDependencies)
            {
                dep.ResetKeys();
            }

            foreach (var stepColumn in DexihDatalinkStepColumns)
            {
                stepColumn.ResetKeys();
            }
        }

    }
}
