using System.Collections.Generic;


using Dexih.Utils.CopyProperties;
using System.Linq;
using System.Text.Json.Serialization;
using dexih.functions.Query;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatajob : DexihHubNamedEntity
    {

        public DexihDatajob()
        {
            DexihDatalinkSteps = new HashSet<DexihDatalinkStep>();
            DexihTriggers = new HashSet<DexihTrigger>();
            Parameters = new HashSet<DexihDatajobParameter>();
        }

        [Key(7)]
        public EFailAction FailAction { get; set; }

        [Key(8)]
        public long? AuditConnectionKey { get; set; }

        /// <summary>
        /// Indicates if the job should watch for any new files.
        /// </summary>
        [Key(9)]
        public bool FileWatch { get; set; }

        [Key(10)]
        public bool AutoStart { get; set; }

        /// <summary>
        /// Indicates if the job can be triggered through the external api.
        /// </summary>
        // public bool ExternalTrigger { get; set; }

        [Key(11)]
        public virtual ICollection<DexihDatalinkStep> DexihDatalinkSteps { get; set; }

        [Key(12)]
        public virtual ICollection<DexihTrigger> DexihTriggers { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihConnection AuditConnection { get; set; }

        [Key(13)]
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
