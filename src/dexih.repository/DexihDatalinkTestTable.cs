using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkTestTable: DexihHubNamedEntity
    {
        [Key(7)]
        [CopyParentCollectionKey]
        public long DatalinkTestStepKey { get; set; }

        [Key(8)]
        public ETestTableAction Action { get; set; }

        /// <summary>
        /// The original table key for the table definitions.
        /// </summary>
        [Key(9)]
        public long TableKey { get; set; }

        /// <summary>
        /// The connection key where the test data should be pulled from
        /// </summary>
        [Key(10)]
        public long SourceConnectionKey { get; set; }

        [Key(11)]
        public string SourceSchema { get; set; }

        /// <summary>
        /// The table name containing the test data.
        /// </summary>
        [Key(12)]
        public string SourceTableName { get; set; }

        /// <summary>
        /// The connection key where the test data should be loaded
        /// </summary>
        [Key(13)]
        public long TestConnectionKey { get; set; }

        [Key(14)]
        public string TestSchema { get; set; }

        /// <summary>
        /// The name of the table where the test data should be loaded.
        /// </summary>
        [Key(15)]
        public string TestTableName { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihDatalinkTestStep DatalinkTestStep { get; set; }
        
        public override void ResetKeys()
        {
            Key = 0;
        }
    }
}