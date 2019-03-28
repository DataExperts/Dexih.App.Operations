using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatalinkTestStep: DexihHubBaseEntity
    {
        public DexihDatalinkTestStep() => DexihDatalinkTestTables = new HashSet<DexihDatalinkTestTable>();
        
        
        [CopyCollectionKey((long)0, true)]
        public long DatalinkTestStepKey { get; set; }
        
        public long Position { get; set; }
        
        [CopyParentCollectionKey]
        public long DatalinkTestKey { get; set; }
        
        public long DatalinkKey { get; set; }
        
        public string Name { get; set; }
        public string Description { get; set; }

        public long TargetConnectionKey { get; set; }
        public string TargetTableName { get; set; }
        public string TargetSchema { get; set; }
        
        public long ExpectedConnectionKey { get; set; }
        public string ExpectedTableName { get; set; }
        public string ExpectedSchema { get; set; }

        public ICollection<DexihDatalinkTestTable> DexihDatalinkTestTables { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkTest DatalinkTest { get; set; }

    }
}