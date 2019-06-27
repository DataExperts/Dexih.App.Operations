using System;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [Serializable]
    public class DexihBaseEntity
    {
        [CopyIfTargetDefault]
        public DateTime CreateDate { get; set; }
        public DateTime UpdateDate { get; set; }

        [CopyIsValid]
        public bool IsValid { get; set; } = true;
    }
}