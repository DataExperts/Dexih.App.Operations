using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihDatalinkProfile : DexihBaseEntity
    {
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }

        [CopyCollectionKey((long)0, true)]
        public long DatalinkProfileKey { get; set; }
		[CopyParentCollectionKey]
        public long DatalinkKey { get; set; }
        public long ProfileRuleKey { get; set; }
        public bool DetailedResults { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalink Datalink { get; set; }
		public virtual DexihProfileRule ProfileRule { get; set; }
    }
}
