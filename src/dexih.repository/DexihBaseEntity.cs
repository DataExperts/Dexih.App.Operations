using System;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    [ProtoInclude(100, typeof(DexihHub))]
    [ProtoInclude(200, typeof(DexihHubUser))]
    [ProtoInclude(300, typeof(DexihRemoteAgent))]
    [ProtoInclude(10000, typeof(DexihHubEntity))]

    public class DexihBaseEntity
    {
        [ProtoMember(1)]
        [CopyIfTargetDefault]
        public DateTime CreateDate { get; set; }

        [ProtoMember(2)]
        public DateTime UpdateDate { get; set; }


        [ProtoMember(3)]
        [CopyIsValid]
        public bool IsValid { get; set; } = true;
    }
}