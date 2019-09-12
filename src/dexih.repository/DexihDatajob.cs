using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;
using System.Linq;
using dexih.functions.Query;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihDatajob : DexihHubNamedEntity
    {

        public DexihDatajob()
        {
            DexihDatalinkSteps = new HashSet<DexihDatalinkStep>();
            DexihTriggers = new HashSet<DexihTrigger>();
            Parameters = new HashSet<DexihDatajobParameter>();
        }

        [ProtoMember(1)]
        public EFailAction FailAction { get; set; }

        [ProtoMember(2)]
        public long? AuditConnectionKey { get; set; }

        /// <summary>
        /// Indicates if the job should watch for any new files.
        /// </summary>
        [ProtoMember(3)]
        public bool FileWatch { get; set; }

        [ProtoMember(4)]
        public bool AutoStart { get; set; }

        /// <summary>
        /// Indicates if the job can be triggered through the external api.
        /// </summary>
        // public bool ExternalTrigger { get; set; }

        [ProtoMember(5)]
        public virtual ICollection<DexihDatalinkStep> DexihDatalinkSteps { get; set; }

        [ProtoMember(6)]
        public virtual ICollection<DexihTrigger> DexihTriggers { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihConnection AuditConnection { get; set; }

        [ProtoMember(7)]
        public ICollection<DexihDatajobParameter> Parameters { get; set; }

        public DexihDatalinkStep GetDatalinkStep(long datalinkStepKey) => DexihDatalinkSteps.SingleOrDefault(step => step.Key == datalinkStepKey);
        public DexihTrigger GetTrigger(long triggerKey) => DexihTriggers.SingleOrDefault(trigger => trigger.Key == triggerKey);
        
        public void UpdateParameters(InputParameters inputParameters)
        {
            if (inputParameters == null || inputParameters.Count == 0 || Parameters == null || Parameters.Count == 0)
            {
                return;
            }

            foreach (var parameter in Parameters)
            {
                var inputParameter = inputParameters.SingleOrDefault(c => c.Name == parameter.Name);
                if (inputParameter != null)
                {
                    parameter.Value = inputParameter.Value;
                }
            }
        }
    }
}
