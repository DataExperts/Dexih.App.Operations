using Dexih.Utils.CopyProperties;
using System.Collections.Generic;
using System.Linq;


using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public partial class DexihHub : DexihBaseEntity
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
        }

        [Key(3)]
        [CopyCollectionKey((long)0, true)]
        public long HubKey { get; set; }

        [Key(4)]
        public string Name { get; set; }

        [Key(5)]
        public string Description { get; set; }

        [Key(6)]
        public string EncryptionKey { get; set; }

        [Key(7)] 
        public ESharedAccess SharedAccess { get; set; } = ESharedAccess.Public;

        [Key(8)]
        public ICollection<DexihConnection> DexihConnections { get; set; }

        [Key(9)]
        public ICollection<DexihTable> DexihTables { get; set; }

        [Key(10)]
        public ICollection<DexihDatajob> DexihDatajobs { get; set; }

        [Key(11)]
        public ICollection<DexihDatalink> DexihDatalinks { get; set; }

        [Key(12)]
        public ICollection<DexihHubUser> DexihHubUsers { get; set; }

        [Key(13)]
        public ICollection<DexihFileFormat> DexihFileFormats { get; set; }

        [Key(14)]
        public ICollection<DexihHubVariable> DexihHubVariables { get; set; }

        [Key(15)]
        public ICollection<DexihDatalinkTest> DexihDatalinkTests { get; set; }

        [Key(16)]
        public ICollection<DexihView> DexihViews { get; set; }

        [Key(17)]
        public ICollection<DexihDashboard> DexihDashboards { get; set; }

        [Key(18)]
        public ICollection<DexihApi> DexihApis { get; set; }

        [Key(19)]
        public ICollection<DexihColumnValidation> DexihColumnValidations { get; set; }

        [Key(20)]
        public ICollection<DexihCustomFunction> DexihCustomFunctions { get; set; }

        [Key(21)]
        public ICollection<DexihRemoteAgentHub> DexihRemoteAgentHubs { get; set; }

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
