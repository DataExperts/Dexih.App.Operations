using ProtoBuf;
using System;
using System.Collections.Generic;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihDatalinkTest: DexihHubNamedEntity
    {
        public DexihDatalinkTest() => DexihDatalinkTestSteps = new HashSet<DexihDatalinkTestStep>();

        [ProtoMember(1)]
        public long? AuditConnectionKey { get; set; }

        [ProtoMember(2)]
        public ICollection<DexihDatalinkTestStep> DexihDatalinkTestSteps { get; set; }
    }
}