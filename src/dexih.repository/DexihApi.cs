using System;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    [Serializable]
    public class DexihApi: DexihHubNamedEntity
    {        
        public long Key { get; set; }
       
        public string Name { get; set; }
        public string Description { get; set; }
        
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
        
    }
}