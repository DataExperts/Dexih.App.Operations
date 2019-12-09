using System.Collections.Generic;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkTestStep: DexihHubNamedEntity
    {
        public DexihDatalinkTestStep() => DexihDatalinkTestTables = new HashSet<DexihDatalinkTestTable>();

        [Key(7)]
        public long Position { get; set; }

        [Key(8)]
        [CopyParentCollectionKey]
        public long DatalinkTestKey { get; set; }

        [Key(9)]
        public long DatalinkKey { get; set; }

        [Key(10)]
        public long TargetConnectionKey { get; set; }

        [Key(11)]
        public string TargetTableName { get; set; }

        [Key(12)]
        public string TargetSchema { get; set; }

        [Key(13)]
        public long ExpectedConnectionKey { get; set; }

        [Key(14)]
        public string ExpectedTableName { get; set; }

        [Key(15)]
        public string ExpectedSchema { get; set; }

        [Key(14)]
        public long ErrorConnectionKey { get; set; }

        [Key(15)]
        public string ErrorTableName { get; set; }

        [Key(16)]
        public string ErrorSchema { get; set; }

        [Key(17)]
        public ICollection<DexihDatalinkTestTable> DexihDatalinkTestTables { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihDatalinkTest DatalinkTest { get; set; }
        
        public override void ResetKeys()
        {
            Key = 0;
            
            foreach (var table in DexihDatalinkTestTables)
            {
                table.ResetKeys();
            }
        }

    }
}