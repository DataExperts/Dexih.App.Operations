using System;
using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihDatalinkTest: DexihBaseEntity
    {
        public DexihDatalinkTest() => DexihDatalinkTestSteps = new HashSet<DexihDatalinkTestStep>();

        public long HubKey { get; set; }
        
        [CopyCollectionKey((long)0, true)]
        public long DatalinkTestKey { get; set; }
        
        public string Name { get; set; }
        public string Description { get; set; }
        
        public long? AuditConnectionKey { get; set; }
        
        public ICollection<DexihDatalinkTestStep> DexihDatalinkTestSteps { get; set; }
    }
}