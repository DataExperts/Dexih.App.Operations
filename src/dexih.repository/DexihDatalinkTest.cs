
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace dexih.repository
{
    [DataContract]
    public class DexihDatalinkTest: DexihHubNamedEntity
    {
        public DexihDatalinkTest() => DexihDatalinkTestSteps = new HashSet<DexihDatalinkTestStep>();

        [DataMember(Order = 7)]
        public long? AuditConnectionKey { get; set; }

        [DataMember(Order = 8)]
        public ICollection<DexihDatalinkTestStep> DexihDatalinkTestSteps { get; set; }
        
        public override void ResetKeys()
        {
            base.ResetKeys();
            
            foreach (var step in DexihDatalinkTestSteps)
            {
                step.ResetKeys();
            }
        }
    }
}