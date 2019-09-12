using System;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihDatalinkDependency : DexihHubNamedEntity
    {
        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DatalinkStepKey { get; set; }

        [ProtoMember(2)]
        public long DependentDatalinkStepKey { get; set; }

        [JsonIgnore, CopyIgnore]
         public virtual DexihDatalinkStep DatalinkStep { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkStep DependentDatalinkStep { get; set; }


    }
}
