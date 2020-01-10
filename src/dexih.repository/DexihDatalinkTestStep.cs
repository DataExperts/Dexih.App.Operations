using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkTestStep: DexihHubNamedEntity
    {
        public DexihDatalinkTestStep() => DexihDatalinkTestTables = new HashSet<DexihDatalinkTestTable>();

        [DataMember(Order = 7)]
        public long Position { get; set; }

        [DataMember(Order = 8)]
        [CopyParentCollectionKey]
        public long DatalinkTestKey { get; set; }

        [DataMember(Order = 9)]
        public long DatalinkKey { get; set; }

        [DataMember(Order = 10)]
        public long TargetConnectionKey { get; set; }

        [DataMember(Order = 11)]
        public string TargetTableName { get; set; }

        [DataMember(Order = 12)]
        public string TargetSchema { get; set; }

        [DataMember(Order = 13)]
        public long ExpectedConnectionKey { get; set; }

        [DataMember(Order = 14)]
        public string ExpectedTableName { get; set; }

        [DataMember(Order = 15)]
        public string ExpectedSchema { get; set; }

        [DataMember(Order = 14)]
        public long ErrorConnectionKey { get; set; }

        [DataMember(Order = 15)]
        public string ErrorTableName { get; set; }

        [DataMember(Order = 16)]
        public string ErrorSchema { get; set; }

        [DataMember(Order = 17)]
        public ICollection<DexihDatalinkTestTable> DexihDatalinkTestTables { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
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