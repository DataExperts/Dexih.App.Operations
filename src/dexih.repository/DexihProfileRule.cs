using System.Collections.Generic;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihProfileRule : DexihBaseEntity
    {
        public DexihProfileRule()
        {
            DexihDatalinkProfiles = new HashSet<DexihDatalinkProfile>();
        }

        public long ProfileRuleKey { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string Class { get; set; }
        public string Assembly { get; set; }
        public string Method { get; set; }
        public string ResultMethod { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalinkProfile> DexihDatalinkProfiles { get; set; }
    }
}
