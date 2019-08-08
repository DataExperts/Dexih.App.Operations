using System;
using Dexih.Utils.CopyProperties;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.repository
{
    [Serializable]
    public partial class DexihHub : DexihBaseEntity
    {
        /// <summary>
        /// Level of access required to view shared hub data.
        /// </summary>
        [JsonConverter(typeof (StringEnumConverter))]
        public enum  ESharedAccess
        {
            Public, // shared objects can be accessed by public
            Registered, // shared objects can be accessed by registred users only 
            Reader // shared objects can be access only be users with PublishReader permission
        }
        
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
        }

        [CopyCollectionKey((long)0, true)]
        public long HubKey { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string EncryptionKey { get; set; }

        public ESharedAccess SharedAccess { get; set; }
        
 //       public bool IsInternal { get; set; }

        public ICollection<DexihConnection> DexihConnections { get; set; }
        public ICollection<DexihTable> DexihTables { get; set; }
        public ICollection<DexihDatajob> DexihDatajobs { get; set; }
        public ICollection<DexihDatalink> DexihDatalinks { get; set; }
        public ICollection<DexihHubUser> DexihHubUsers { get; set; }
        public ICollection<DexihFileFormat> DexihFileFormats { get; set; }
        public ICollection<DexihHubVariable> DexihHubVariables { get; set; }
        public ICollection<DexihDatalinkTest> DexihDatalinkTests { get; set; }
        public ICollection<DexihView> DexihViews { get; set; }
        public ICollection<DexihApi> DexihApis { get; set; }

        public ICollection<DexihColumnValidation> DexihColumnValidations { get; set; }
        public ICollection<DexihCustomFunction> DexihCustomFunctions { get; set; }
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
                var column = table.DexihTableColumns.SingleOrDefault(c => c.Key == columnKey);
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
            var table = DexihTables.SingleOrDefault(c=>c.Key == tableKey && IsValid);
            return table;
        }
        
    }
}
