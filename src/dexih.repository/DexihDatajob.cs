using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;
using System.Linq;

namespace dexih.repository
{
    public partial class DexihDatajob : DexihHubBaseEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EFailAction
        {
            Continue,
            ContinueNonDependent,
            Abend
        }
        public DexihDatajob()
        {
            DexihDatalinkSteps = new HashSet<DexihDatalinkStep>();
            DexihTriggers = new HashSet<DexihTrigger>();
        }

		[CopyCollectionKey((long)0, true)]
        public long DatajobKey { get; set; }

        public string Name { get; set; }
        public string Description { get; set; }

        public EFailAction FailAction { get; set; }

        public long? AuditConnectionKey { get; set; }

        /// <summary>
        /// Indicates if the job should watch for any new files.
        /// </summary>
        public bool FileWatch { get; set; }
        
        public bool AutoStart { get; set; }
        
        /// <summary>
        /// Indicates if the job can be triggered through the external api.
        /// </summary>
        // public bool ExternalTrigger { get; set; }

        public virtual ICollection<DexihDatalinkStep> DexihDatalinkSteps { get; set; }
        public virtual ICollection<DexihTrigger> DexihTriggers { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihConnection AuditConnection { get; set; }

        public DexihDatalinkStep GetDatalinkStep(long datalinkStepKey) => DexihDatalinkSteps.SingleOrDefault(step => step.DatalinkStepKey == datalinkStepKey);
        public DexihTrigger GetTrigger(long triggerKey) => DexihTriggers.SingleOrDefault(trigger => trigger.TriggerKey == triggerKey);
    }
}
