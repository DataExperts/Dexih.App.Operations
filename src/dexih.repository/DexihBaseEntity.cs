using System;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    [Union(0, typeof(DexihHub))]
    [Union(1, typeof(DexihHubUser))]
    [Union(2, typeof(DexihRemoteAgent))]
    [Union(3, typeof(DexihHubEntity))]

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