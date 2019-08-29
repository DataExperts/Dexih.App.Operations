using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.repository
{
    [Serializable]
    public class DexihView : DexihHubNamedEntity
    {
        public DexihView()
        {
            Parameters = new HashSet<DexihViewParameter>();
        }
        [JsonConverter(typeof(StringEnumConverter))]
        public enum EViewType
        {
            Table, Chart            
        }
        
        public EViewType ViewType { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalink SourceDatalink { get; set; }

        public ESourceType SourceType { get; set; }
        
        public bool AutoRefresh { get; set; }
        
        [NotMapped]
        [CopyReference]
        public InputColumn[] InputValues { get; set; }
        
        [CopyReference]
        public SelectQuery SelectQuery { get; set; }
        [CopyReference]
        public ChartConfig ChartConfig { get; set; }
        
        public bool IsShared { get; set; }
        
        public ICollection<DexihViewParameter> Parameters { get; set; }

    }

}