using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Security.Permissions;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.repository
{
    public class DexihView : DexihBaseEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EViewType
        {
            Table, Chart            
        }
        
        public long ViewKey { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
        
        public string Name { get; set; }
        public string Description { get; set; }
        
        public EViewType ViewType { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }
	    
        public ESourceType SourceType { get; set; }
        
        [NotMapped]
        [CopyReference]
        public InputColumn[] InputValues { get; set; }
        
        [CopyReference]
        public SelectQuery SelectQuery { get; set; }
        [CopyReference]
        public ChartConfig ChartConfig { get; set; }
    }

}