using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDashboardItem: DexihHubNamedEntity
    {
        public DexihDashboardItem()
        {
            Parameters = new HashSet<DexihDashboardItemParameter>();
        }

        [DataMember(Order = 7)]
        public int Cols { get; set; }

        [DataMember(Order = 8)]
        public int Rows { get; set; }

        [DataMember(Order = 9)]
        public int X { get; set; }

        [DataMember(Order = 10)]
        public int Y { get; set; }

        [DataMember(Order = 11)]
        public bool Header { get; set; }

        [DataMember(Order = 12)]
        public bool Scrollable { get; set; }

        [DataMember(Order = 13)]
        public long ViewKey { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihView View { get; set; }

        [DataMember(Order = 14)]
        [CopyParentCollectionKey(nameof(Key))]
        public long DashboardKey { get; set; }

        [DataMember(Order = 15)]
        public ICollection<DexihDashboardItemParameter> Parameters { get; set; }
        
       
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDashboard Dashboard { get; set; }

        public override void ResetKeys()
        {
            base.ResetKeys();
            
            foreach (var parameter in Parameters)
            {
                parameter.ResetKeys();
            }
        }
    }
}