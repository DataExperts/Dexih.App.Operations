using System;
using Newtonsoft.Json;
using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public class DexihTrigger : DexihHubBaseEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EDayOfWeek
        {
            Sunday = 0,
            Monday = 1,
            Tuesday = 2,
            Wednesday = 3,
            Thursday = 4,
            Friday = 5,
            Saturday = 6
        }


        [CopyCollectionKey((long)0, true)]
        public long TriggerKey { get; set; }
		
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }
        public DateTime? StartDate { get; set; }
        public TimeSpan? IntervalTime { get; set; }

        [CopyIgnore] 
        public EDayOfWeek[] DaysOfWeek { get; set; }

        public TimeSpan? StartTime { get; set; }
        public TimeSpan? EndTime { get; set; }
        public string CronExpression { get; set; }
        public int? MaxRecurs { get; set; }

        [NotMapped]
        public string Details
        {
            get
            {
                var desc = new StringBuilder();

                if (StartDate != null)
                    desc.AppendLine("Starts on/after:" + StartDate);
                if (StartTime != null)
                    desc.AppendLine("Runs daily after:" + StartTime.Value.ToString());
                if (EndTime != null)
                    desc.AppendLine("Ends daily after:" + EndTime.Value.ToString());
                if (DaysOfWeek.Length > 0 && DaysOfWeek.Length < 7)
                    desc.AppendLine("Only on:" + String.Join(",", DaysOfWeek.Select(c => c.ToString()).ToArray()));
                if (IntervalTime != null)
                    desc.AppendLine("Runs every: " + IntervalTime.Value.ToString());
                if (MaxRecurs != null)
                    desc.AppendLine("Recurs for: " + MaxRecurs.Value.ToString());

                return desc.ToString();
            }
        }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatajob Datajob { get; set; }
        
    }
}
