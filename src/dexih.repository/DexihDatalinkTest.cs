using MessagePack;
using System;
using System.Collections.Generic;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihDatalinkTest: DexihHubNamedEntity
    {
        public DexihDatalinkTest() => DexihDatalinkTestSteps = new HashSet<DexihDatalinkTestStep>();

        [Key(7)]
        public long? AuditConnectionKey { get; set; }

        [Key(8)]
        public ICollection<DexihDatalinkTestStep> DexihDatalinkTestSteps { get; set; }
    }
}