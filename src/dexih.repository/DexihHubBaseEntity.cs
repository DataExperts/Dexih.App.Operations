using System;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihHubBaseEntity: DexihBaseEntity
    {
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
    }
}
