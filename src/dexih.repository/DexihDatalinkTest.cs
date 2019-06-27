using System;
using System.Collections.Generic;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    [Serializable]
    public class DexihDatalinkTest: DexihHubNamedEntity
    {
        public DexihDatalinkTest() => DexihDatalinkTestSteps = new HashSet<DexihDatalinkTestStep>();

        
        public long? AuditConnectionKey { get; set; }
        
        public ICollection<DexihDatalinkTestStep> DexihDatalinkTestSteps { get; set; }
    }
}