using System;
using Dexih.Utils.CopyProperties;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
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

        [ProtoMember(1)]
        [CopyCollectionKey((long)0, true)]
        public long HubKey { get; set; }

        [ProtoMember(2)]
        public string Name { get; set; }

        [ProtoMember(3)]
        public string Description { get; set; }

        [ProtoMember(4)]
        public string EncryptionKey { get; set; }

        [ProtoMember(5)]
        public ESharedAccess SharedAccess { get; set; }

        [ProtoMember(6)]
        public ICollection<DexihConnection> DexihConnections { get; set; }

        [ProtoMember(7)]
        public ICollection<DexihTable> DexihTables { get; set; }

        [ProtoMember(8)]
        public ICollection<DexihDatajob> DexihDatajobs { get; set; }

        [ProtoMember(9)]
        public ICollection<DexihDatalink> DexihDatalinks { get; set; }

        [ProtoMember(10)]
        public ICollection<DexihHubUser> DexihHubUsers { get; set; }

        [ProtoMember(11)]
        public ICollection<DexihFileFormat> DexihFileFormats { get; set; }

        [ProtoMember(12)]
        public ICollection<DexihHubVariable> DexihHubVariables { get; set; }

        [ProtoMember(13)]
        public ICollection<DexihDatalinkTest> DexihDatalinkTests { get; set; }

        [ProtoMember(14)]
        public ICollection<DexihView> DexihViews { get; set; }

        [ProtoMember(15)]
        public ICollection<DexihDashboard> DexihDashboards { get; set; }

        [ProtoMember(16)]
        public ICollection<DexihApi> DexihApis { get; set; }

        [ProtoMember(17)]
        public ICollection<DexihColumnValidation> DexihColumnValidations { get; set; }

        [ProtoMember(18)]
        public ICollection<DexihCustomFunction> DexihCustomFunctions { get; set; }

        [ProtoMember(19)]
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
