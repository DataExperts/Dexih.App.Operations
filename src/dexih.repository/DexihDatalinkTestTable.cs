using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkTestTable: DexihHubNamedEntity
    {
        [DataMember(Order = 7)]
        [CopyParentCollectionKey]
        public long DatalinkTestStepKey { get; set; }

        [DataMember(Order = 8)]
        public ETestTableAction Action { get; set; }

        /// <summary>
        /// The original table key for the table definitions.
        /// </summary>
        [DataMember(Order = 9)]
        public long TableKey { get; set; }

        /// <summary>
        /// The connection key where the test data should be pulled from
        /// </summary>
        [DataMember(Order = 10)]
        public long SourceConnectionKey { get; set; }

        [DataMember(Order = 11)]
        public string SourceSchema { get; set; }

        /// <summary>
        /// The table name containing the test data.
        /// </summary>
        [DataMember(Order = 12)]
        public string SourceTableName { get; set; }

        /// <summary>
        /// The connection key where the test data should be loaded
        /// </summary>
        [DataMember(Order = 13)]
        public long TestConnectionKey { get; set; }

        [DataMember(Order = 14)]
        public string TestSchema { get; set; }

        /// <summary>
        /// The name of the table where the test data should be loaded.
        /// </summary>
        [DataMember(Order = 15)]
        public string TestTableName { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public virtual DexihDatalinkTestStep DatalinkTestStep { get; set; }
    }
}