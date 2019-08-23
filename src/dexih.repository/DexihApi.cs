using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    [Serializable]
    public class DexihApi: DexihHubNamedEntity
    {
        public DexihApi()
        {
            Parameters = new HashSet<DexihApiParameter>();
        }
        public ESourceType SourceType { get; set; }
        
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }
	    
        public bool AutoStart { get; set; }

        public bool CacheQueries { get; set; }
        public TimeSpan? CacheResetInterval { get; set; }

        public string LogDirectory { get; set; }
        
        [CopyReference]
        public SelectQuery SelectQuery { get; set; }
        
        public ICollection<DexihApiParameter> Parameters { get; set; }
        
        public void UpdateParameters(InputParameters inputParameters)
        {
            if (inputParameters == null || inputParameters.Count == 0 || Parameters == null || Parameters.Count == 0)
            {
                return;
            }

            foreach (var parameter in Parameters)
            {
                var inputParameter = inputParameters.SingleOrDefault(c => c.Name == parameter.Name);
                if (inputParameter != null)
                {
                    parameter.Value = inputParameter.Value;
                }
            }
        }
    }
}