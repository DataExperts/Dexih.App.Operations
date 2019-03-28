using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatalinkTestTable: DexihHubBaseEntity
    {
        public enum ETestTableAction
        {
            None,
            Truncate,
            DropCreate,
            TruncateCopy,
            DropCreateCopy
        }
        
        [CopyCollectionKey((long)0, true)]
        public long DatalinkTestTableKey { get; set; }
        
        [CopyParentCollectionKey]
        public long DatalinkTestStepKey { get; set; }

        
        public ETestTableAction Action { get; set; }
        /// <summary>
        /// The original table key for the table definitions.
        /// </summary>
        public long TableKey { get; set; }

        /// <summary>
        /// The connection key where the test data should be pulled from
        /// </summary>
        public long SourceConnectionKey { get; set; }
        
        public string SourceSchema { get; set; }

        /// <summary>
        /// The table name containing the test data.
        /// </summary>
        public string SourceTableName { get; set; }
        
        /// <summary>
        /// The connection key where the test data should be loaded
        /// </summary>
        public long TestConnectionKey { get; set; }

        public string TestSchema { get; set; }

        /// <summary>
        /// The name of the table where the test data should be loaded.
        /// </summary>
        public string TestTableName { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkTestStep DatalinkTestStep { get; set; }
    }
}