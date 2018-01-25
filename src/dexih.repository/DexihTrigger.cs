using System;
using Newtonsoft.Json;
using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public class DexihTrigger : DexihBaseEntity
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

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        [CopyCollectionKey((long)0, true)]
        public long TriggerKey { get; set; }
		
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }
        public DateTime? StartDate { get; set; }
        public TimeSpan? IntervalTime { get; set; }
        
        [JsonIgnore]
        public string DaysOfWeekString { get; set; }
        
        [NotMapped, CopyIgnore]
        public EDayOfWeek[] DaysOfWeek {
            get { return string.IsNullOrEmpty(DaysOfWeekString) ? new EDayOfWeek[] { } : DaysOfWeekString.Split(',').Select(c => (EDayOfWeek)Enum.Parse(typeof(EDayOfWeek), c)).ToArray(); }
            set { DaysOfWeekString = String.Join(",", value.Select(c=>c.ToString())); }
        }

        public TimeSpan? StartTime { get; set; }
        public TimeSpan? EndTime { get; set; }
        public string CronExpression { get; set; }
        public int? MaxRecurrs { get; set; }

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
                if (MaxRecurrs != null)
                    desc.AppendLine("Recurrs for: " + MaxRecurrs.Value.ToString());

                return desc.ToString();
            }
        }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatajob Datajob { get; set; }
        
    }
}
