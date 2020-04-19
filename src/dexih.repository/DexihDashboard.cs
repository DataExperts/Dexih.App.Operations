
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace dexih.repository
{
    [DataContract]
    public class DexihDashboard: DexihHubNamedEntity
    {
        public DexihDashboard()
        {
            DexihDashboardItems = new HashSet<DexihDashboardItem>();
            Parameters = new HashSet<DexihDashboardParameter>();
        }

        [DataMember(Order = 7)]
        public bool IsShared { get; set; }

        [DataMember(Order = 8)]
        public int MinRows { get; set; }

        [DataMember(Order = 9)]
        public int MinCols { get; set; }

        [DataMember(Order = 10)]
        public int MaxRows { get; set; }

        [DataMember(Order = 11)]
        public int MaxCols { get; set; }

        [DataMember(Order = 12)]
        public bool AutoRefresh { get; set; }

        [DataMember(Order = 13)]
        public ICollection<DexihDashboardItem>  DexihDashboardItems { get; set; }

        [DataMember(Order = 14)]
        public ICollection<DexihDashboardParameter> Parameters { get; set; }
        
        public override void ResetKeys()
        {
            base.ResetKeys();

            foreach (var item in DexihDashboardItems)
            {
                item.ResetKeys();
            }
            
            foreach (var parameter in Parameters)
            {
                parameter.ResetKeys();
            }
        }
    }
}