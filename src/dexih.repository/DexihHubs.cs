using Dexih.Utils.CopyProperties;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.repository
{
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
            DexihDatajobs = new HashSet<DexihDatajob>();
            DexihDatalinks = new HashSet<DexihDatalink>();
            DexihHubUsers = new HashSet<DexihHubUser>();
			DexihFileFormats = new HashSet<DexihFileFormat>();
            DexihHubVariables = new HashSet<DexihHubVariable>();
            DexihCustomFunctions = new HashSet<DexihCustomFunction>();
            DexihColumnValidations = new HashSet<DexihColumnValidation>();
            DexihRemoteAgents = new HashSet<DexihRemoteAgent>();
        }

        [CopyCollectionKey((long)0, true)]
        public long HubKey { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string EncryptionKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public string SharedAccessString
        {
            get => SharedAccess.ToString();
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    SharedAccess = ESharedAccess.Reader;
                }
                else
                {
                    SharedAccess = (ESharedAccess)Enum.Parse(typeof(ESharedAccess), value);
                }
            }
        }        
        [NotMapped]
        public ESharedAccess SharedAccess { get; set; }
        
//		public int? MaxOwners { get; set; }
//        public int? MaxUsers { get; set; }
//        public int? MaxReaders { get; set; }
//        public int? MaxDatalinks { get; set; }
//        public int? MaxDatajobs { get; set; }
//        public int? DailyTransactionQuota { get; set; }
//        public DateTime? ExpiryDate { get; set; }
        public bool IsInternal { get; set; }

        public virtual ICollection<DexihConnection> DexihConnections { get; set; }
        public virtual ICollection<DexihDatajob> DexihDatajobs { get; set; }
        public virtual ICollection<DexihDatalink> DexihDatalinks { get; set; }
        public virtual ICollection<DexihHubUser> DexihHubUsers { get; set; }
        public virtual ICollection<DexihFileFormat> DexihFileFormats { get; set; }
        public virtual ICollection<DexihHubVariable> DexihHubVariables { get; set; }

        public virtual ICollection<DexihColumnValidation> DexihColumnValidations { get; set; }
        public virtual ICollection<DexihCustomFunction> DexihCustomFunctions { get; set; }
        public virtual ICollection<DexihRemoteAgent> DexihRemoteAgents { get; set; }

        /// <summary>
        /// Searches all connections and table for a columnKey.
        /// </summary>
        /// <param name="columnKey"></param>
        /// <returns></returns>
        public DexihTableColumn GetColumnFromKey(long columnKey)
        {
            var column = DexihConnections.SelectMany(c => c.DexihTables).SelectMany(d => d.DexihTableColumns).SingleOrDefault(c => c.ColumnKey == columnKey);
            return column;
        }

        /// <summary>
        /// Searches all connections for a table.
        /// </summary>
        /// <param name="tableKey"></param>
        /// <returns></returns>
        public DexihTable GetTableFromKey(long tableKey)
        {
            var table = DexihConnections.SelectMany(c => c.DexihTables).SingleOrDefault(c=>c.TableKey == tableKey && IsValid);
            return table;
        }
    }
}
