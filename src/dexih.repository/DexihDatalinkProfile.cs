using System;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihDatalinkProfile : DexihHubNamedEntity
    {
        [ProtoMember(1)]
        [CopyParentCollectionKey]
        public long DatalinkKey { get; set; }

        [ProtoMember(2)]
        public string FunctionClassName { get; set; }

        [ProtoMember(3)]
        public string FunctionAssemblyName { get; set; }

        [ProtoMember(4)]
        public string FunctionMethodName { get; set; }

        [ProtoMember(5)]
        public bool DetailedResults { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalink Datalink { get; set; }
    }
}
