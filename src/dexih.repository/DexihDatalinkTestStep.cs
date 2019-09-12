using System;
using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDatalinkTestStep: DexihHubNamedEntity
    {
        public DexihDatalinkTestStep() => DexihDatalinkTestTables = new HashSet<DexihDatalinkTestTable>();

        [ProtoMember(1)]
        public long Position { get; set; }

        [ProtoMember(2)]
        [CopyParentCollectionKey]
        public long DatalinkTestKey { get; set; }

        [ProtoMember(3)]
        public long DatalinkKey { get; set; }

        [ProtoMember(4)]
        public long TargetConnectionKey { get; set; }

        [ProtoMember(5)]
        public string TargetTableName { get; set; }

        [ProtoMember(6)]
        public string TargetSchema { get; set; }

        [ProtoMember(7)]
        public long ExpectedConnectionKey { get; set; }

        [ProtoMember(8)]
        public string ExpectedTableName { get; set; }

        [ProtoMember(9)]
        public string ExpectedSchema { get; set; }

        [ProtoMember(10)]
        public ICollection<DexihDatalinkTestTable> DexihDatalinkTestTables { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkTest DatalinkTest { get; set; }

    }
}