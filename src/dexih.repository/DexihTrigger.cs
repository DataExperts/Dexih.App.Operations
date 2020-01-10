using System;
using System.Linq;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.ManagedTasks;


namespace dexih.repository
{
    [DataContract]
    public class DexihTrigger : DexihHubNamedEntity
    {
        [DataMember(Order = 7)]
        [CopyParentCollectionKey]
        public long DatajobKey { get; set; }

        [DataMember(Order = 8)]
        public DateTime? StartDate { get; set; }

        [DataMember(Order = 9)]
        public TimeSpan? IntervalTime { get; set; }
        
        [DataMember(Order = 10)]
        public int Position { get; set; }

        [DataMember(Order = 11)]
        [NotMapped, CopyIgnore]
        public EDayOfWeek[] DaysOfWeek
        {
            get
            {
                if (string.IsNullOrEmpty(DaysOfWeekString))
                {
                    return null;
                }
                else
                {
                    return DaysOfWeekString.Split(',')
                        .Select(c => (EDayOfWeek) Enum.Parse(typeof(EDayOfWeek), c)).ToArray();
                }
            }
            set
            {
                if (value == null)
                {
                    DaysOfWeekString = null;
                }
                else
                {
                    DaysOfWeekString = String.Join(",", value.Select(c => c.ToString()));
                }
            }
        }

        [JsonIgnore]
        public string DaysOfWeekString { get; set; }

        [DataMember(Order = 12)]
        public TimeSpan? StartTime { get; set; }

        [DataMember(Order = 13)]
        public TimeSpan? EndTime { get; set; }

        [DataMember(Order = 14)]
        public string CronExpression { get; set; }

        [DataMember(Order = 15)]
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
                if (DaysOfWeek != null && DaysOfWeek.Length > 0 && DaysOfWeek.Length < 7)
                    desc.AppendLine("Only on:" + String.Join(",", DaysOfWeek.Select(c => c.ToString()).ToArray()));
                if (IntervalTime != null)
                    desc.AppendLine("Runs every: " + IntervalTime.Value.ToString());
                if (MaxRecurs != null)
                    desc.AppendLine("Recurs for: " + MaxRecurs.Value.ToString());

                return desc.ToString();
            }
        }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatajob Datajob { get; set; }

        public ManagedTaskTrigger CreateManagedTaskTrigger()
        {
            var daysOfWeek = DaysOfWeek?.ToArray() ?? Enum.GetValues(typeof(EDayOfWeek)).Cast<EDayOfWeek>().ToArray();
            var managedTaskTrigger = new ManagedTaskTrigger()
            {
                Details =  Description,
                EndDate = null,
                EndTime = EndTime,
                IntervalTime =  IntervalTime,
                DaysOfWeek = daysOfWeek,
                IntervalType = EIntervalType.Interval,
                MaxRecurs =  MaxRecurs,
                SkipDates = null,
                StartDate = StartDate,
                StartTime = StartTime,
                DaysOfMonth = null,
                WeeksOfMonth = null
            };

            return managedTaskTrigger;
        }
        
        public override void ResetKeys()
        {
            Key = 0;
        }
        
    }
}
