using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.ManagedTasks;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihTrigger : DexihHubNamedEntity
    {
        [Key(7)]
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [Key(8)]
        public DateTime? StartDate { get; set; }

        [Key(9)]
        public TimeSpan? IntervalTime { get; set; }

        [Key(10)]
        public List<ManagedTaskSchedule.EDayOfWeek> DaysOfWeek { get; set; }

        [Key(11)]
        public TimeSpan? StartTime { get; set; }

        [Key(12)]
        public TimeSpan? EndTime { get; set; }

        [Key(13)]
        public string CronExpression { get; set; }

        [Key(14)]
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
                if (DaysOfWeek != null && DaysOfWeek.Count > 0 && DaysOfWeek.Count < 7)
                    desc.AppendLine("Only on:" + String.Join(",", DaysOfWeek.Select(c => c.ToString()).ToArray()));
                if (IntervalTime != null)
                    desc.AppendLine("Runs every: " + IntervalTime.Value.ToString());
                if (MaxRecurs != null)
                    desc.AppendLine("Recurs for: " + MaxRecurs.Value.ToString());

                return desc.ToString();
            }
        }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatajob Datajob { get; set; }

        public ManagedTaskSchedule CreateManagedTaskSchedule()
        {
            var managedTaskSchedule = new ManagedTaskSchedule()
            {
                Details =  Description,
                EndDate = null,
                EndTime = EndTime,
                IntervalTime =  IntervalTime,
                DaysOfWeek = DaysOfWeek.ToArray(),
                IntervalType = ManagedTaskSchedule.EIntervalType.Interval,
                MaxRecurs =  MaxRecurs,
                SkipDates = null,
                StartDate = StartDate,
                StartTime = StartTime,
                DaysOfMonth = null,
                WeeksOfMonth = null
            };

            return managedTaskSchedule;
        }
        
    }
}
