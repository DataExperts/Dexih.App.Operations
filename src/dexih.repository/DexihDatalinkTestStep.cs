using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatalinkTestStep: DexihHubNamedEntity
    {
        public DexihDatalinkTestStep() => DexihDatalinkTestTables = new HashSet<DexihDatalinkTestTable>();
        
                
        public long Position { get; set; }
        
        [CopyParentCollectionKey]
        public long DatalinkTestKey { get; set; }
        
        public long DatalinkKey { get; set; }
        
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