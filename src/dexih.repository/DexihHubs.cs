using Dexih.Utils.CopyProperties;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;


namespace dexih.repository
{
    [DataContract]
    public class DexihHub : DexihBaseEntity
    {
       
        public DexihHub()
        {
            DexihConnections = new HashSet<DexihConnection>();
            DexihTables = new HashSet<DexihTable>();
            DexihDatajobs = new HashSet<DexihDatajob>();
            DexihDatalinks = new HashSet<DexihDatalink>();
            DexihHubUsers = new HashSet<DexihHubUser>();
			DexihFileFormats = new HashSet<DexihFileFormat>();
            DexihHubVariables = new HashSet<DexihHubVariable>();
            DexihCustomFunctions = new HashSet<DexihCustomFunction>();
            DexihColumnValidations = new HashSet<DexihColumnValidation>();
            DexihRemoteAgentHubs = new HashSet<DexihRemoteAgentHub>();
            DexihDatalinkTests = new HashSet<DexihDatalinkTest>();
            DexihViews = new HashSet<DexihView>();
            DexihApis = new HashSet<DexihApi>();
            DexihDashboards = new HashSet<DexihDashboard>();
            DexihListOfValues = new HashSet<DexihListOfValues>();
            DexihTags = new HashSet<DexihTag>();
            DexihTagObjects = new HashSet<DexihTagObject>();
        }

        [DataMember(Order = 3)]
        [CopyCollectionKey((long)0, true)]
        public long HubKey { get; set; }

        [DataMember(Order = 4)]
        public string Name { get; set; }

        [DataMember(Order = 5)]
        public string Description { get; set; }

        [DataMember(Order = 6)]
        public string EncryptionKey { get; set; }

        [DataMember(Order = 7)] 
        public ESharedAccess SharedAccess { get; set; } = ESharedAccess.Public;

        [DataMember(Order = 8)]
        public ICollection<DexihConnection> DexihConnections { get; set; }

        [DataMember(Order = 9)]
        public ICollection<DexihTable> DexihTables { get; set; }

        [DataMember(Order = 10)]
        public ICollection<DexihDatajob> DexihDatajobs { get; set; }

        [DataMember(Order = 11)]
        public ICollection<DexihDatalink> DexihDatalinks { get; set; }

        [DataMember(Order = 12)]
        public ICollection<DexihHubUser> DexihHubUsers { get; set; }

        [DataMember(Order = 13)]
        public ICollection<DexihFileFormat> DexihFileFormats { get; set; }

        [DataMember(Order = 14)]
        public ICollection<DexihHubVariable> DexihHubVariables { get; set; }

        [DataMember(Order = 15)]
        public ICollection<DexihDatalinkTest> DexihDatalinkTests { get; set; }

        [DataMember(Order = 16)]
        public ICollection<DexihView> DexihViews { get; set; }

        [DataMember(Order = 17)]
        public ICollection<DexihDashboard> DexihDashboards { get; set; }

        [DataMember(Order = 18)]
        public ICollection<DexihApi> DexihApis { get; set; }

        [DataMember(Order = 19)]
        public ICollection<DexihColumnValidation> DexihColumnValidations { get; set; }

        [DataMember(Order = 20)]
        public ICollection<DexihCustomFunction> DexihCustomFunctions { get; set; }

        [DataMember(Order = 21)]
        public ICollection<DexihRemoteAgentHub> DexihRemoteAgentHubs { get; set; }
        
        [DataMember(Order = 22)]
        public ICollection<DexihListOfValues> DexihListOfValues { get; set; }
        
        [DataMember(Order = 23)]
        public ICollection<DexihTag> DexihTags { get; set; }

        [DataMember(Order = 24)]
        public ICollection<DexihTagObject> DexihTagObjects { get; set; }

        /// <summary>
        /// Searches all connections and table for a columnKey.
        /// </summary>
        /// <param name="columnKey"></param>
        /// <returns></returns>
        public (DexihTable table, DexihTableColumn column) GetTableColumnFromKey(long columnKey)
        {
            foreach (var table in DexihTables)
            {
                var column = table.DexihTableColumns.SingleOrDefault(c => c.IsValid && c.Key == columnKey);
                if (column != null)
                {
                    return (table, column);
                }
            }

            return (null, null);
        }

        public (DexihDashboard dashboard, DexihDashboardItem dashboardItem) GetDashboardItemFromKey(long dashboardItemKey)
        {
            foreach (var dashboard in DexihDashboards)
            {
                var item = dashboard.DexihDashboardItems.SingleOrDefault(c => c.IsValid && c.Key == dashboardItemKey);
                if (item != null)
                {
                    return (dashboard, item);
                }
            }

            return (null, null);
        }

        
        /// <summary>
        /// Searches all connections for a table.
        /// </summary>
        /// <param name="tableKey"></param>
        /// <returns></returns>
        public DexihTable GetTableFromKey(long tableKey)
        {
            var table = DexihTables.SingleOrDefault(c => c.IsValid && c.Key == tableKey && IsValid);
            return table;
        }
        
    }
}
