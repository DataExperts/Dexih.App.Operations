using System;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    [ProtoInherit(10000000)]
    [MessagePack.Union(0, typeof(DexihHub))]
    [MessagePack.Union(1, typeof(DexihHubUser))]
    [MessagePack.Union(2, typeof(DexihRemoteAgent))]
    [MessagePack.Union(3, typeof(DexihHubEntity))]

    public class DexihBaseEntity
    {
        [Key(0)]
        [CopyIfTargetDefault]
        public DateTime CreateDate { get; set; }

        [Key(1)]
        public DateTime UpdateDate { get; set; }


        [Key(2)]
        [CopyIsValid]
        public bool IsValid { get; set; } = true;
    }
}