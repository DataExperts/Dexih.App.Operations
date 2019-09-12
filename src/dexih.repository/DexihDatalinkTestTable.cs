using System;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDatalinkTestTable: DexihHubNamedEntity
    {
        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DatalinkTestStepKey { get; set; }

        [ProtoMember(2)]
        public ETestTableAction Action { get; set; }

        /// <summary>
        /// The original table key for the table definitions.
        /// </summary>
        [ProtoMember(3)]
        public long TableKey { get; set; }

        /// <summary>
        /// The connection key where the test data should be pulled from
        /// </summary>
        [ProtoMember(4)]
        public long SourceConnectionKey { get; set; }

        [ProtoMember(5)]
        public string SourceSchema { get; set; }

        /// <summary>
        /// The table name containing the test data.
        /// </summary>
        [ProtoMember(6)]
        public string SourceTableName { get; set; }

        /// <summary>
        /// The connection key where the test data should be loaded
        /// </summary>
        [ProtoMember(7)]
        public long TestConnectionKey { get; set; }

        [ProtoMember(8)]
        public string TestSchema { get; set; }

        /// <summary>
        /// The name of the table where the test data should be loaded.
        /// </summary>
        [ProtoMember(9)]
        public string TestTableName { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkTestStep DatalinkTestStep { get; set; }
    }
}