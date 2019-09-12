using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihHubUser : DexihBaseEntity
    {


        [ProtoMember(1)]
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        [ProtoMember(2)]
        public string UserId { get; set; }

        [ProtoMember(3)]
        public EPermission Permission { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }
    }
}
