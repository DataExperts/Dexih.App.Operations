using System.Collections.Generic;


using Dexih.Utils.CopyProperties;
using System.Linq;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.functions.Query;


namespace dexih.repository
{
    [DataContract]
    public sealed class DexihDatajob : DexihHubNamedEntity
    {

        public DexihDatajob()
        {
            DexihDatalinkSteps = new HashSet<DexihDatalinkStep>();
            DexihTriggers = new HashSet<DexihTrigger>();
            Parameters = new HashSet<DexihDatajobParameter>();
        }

        [DataMember(Order = 7)] public EFailAction FailAction { get; set; } = EFailAction.Abend;

        [DataMember(Order = 8)]
        public long? AuditConnectionKey { get; set; }

        /// <summary>
        /// Indicates if the job should watch for any new files.
        /// </summary>
        [DataMember(Order = 9)]
        public bool FileWatch { get; set; }

        [DataMember(Order = 10)]
        public bool AutoStart { get; set; }

        /// <summary>
        /// Indicates if the job can be triggered through the external api.
        /// </summary>
        // public bool ExternalTrigger { get; set; }

        [DataMember(Order = 11)]
        public ICollection<DexihDatalinkStep> DexihDatalinkSteps { get; set; }

        [DataMember(Order = 12)]
        public ICollection<DexihTrigger> DexihTriggers { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihConnection AuditConnection { get; set; }

        [DataMember(Order = 13)]
        public ICollection<DexihDatajobParameter> Parameters { get; set; }

        [DataMember(Order = 14)]
        public EAlertLevel AlertLevel { get; set; }

        public override void ResetKeys()
        {
            base.ResetKeys();
            
            foreach (var parameter in Parameters)
            {
                parameter.ResetKeys();
            }

            foreach (var step in DexihDatalinkSteps)
            {
                step.ResetKeys();
            }

            foreach (var trigger in DexihTriggers)
            {
                trigger.ResetKeys();
            }
        }
        
        public DexihDatalinkStep GetDatalinkStep(long datalinkStepKey) => DexihDatalinkSteps.SingleOrDefault(step => step.IsValid &&  step.Key == datalinkStepKey);
        public DexihTrigger GetTrigger(long triggerKey) => DexihTriggers.SingleOrDefault(trigger => trigger.IsValid &&  trigger.Key == triggerKey);
        
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
