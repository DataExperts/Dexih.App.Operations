using System;
using System.Runtime.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    // [Union(0, typeof(DexihHub))]
    // [Union(1, typeof(DexihHubUser))]
    // [Union(2, typeof(DexihRemoteAgent))]
    // [Union(3, typeof(DexihHubEntity))]

    public class DexihBaseEntity
    {
        [DataMember(Order = 0)]
        [CopyIfTargetDefault]
        public DateTime CreateDate { get; set; }

        [DataMember(Order = 1)]
        public DateTime UpdateDate { get; set; }


        [DataMember(Order = 2)]
        [CopyIsValid]
        public bool IsValid { get; set; } = true;
    }
}