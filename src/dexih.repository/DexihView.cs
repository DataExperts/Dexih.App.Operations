using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;


using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihView : DexihHubNamedEntity
    {
        public DexihView()
        {
            Parameters = new HashSet<DexihViewParameter>();
        }
        
        [Key(7)] 
        public EViewType ViewType { get; set; } = EViewType.Table;

        [Key(8)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [Key(9)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatalink SourceDatalink { get; set; }

        [Key(10)]
        public EDataObjectType SourceType { get; set; }

        [Key(11)] public bool AutoRefresh { get; set; } = true;

        [Key(12)]
        [NotMapped]
        [CopyReference]
        public InputColumn[] InputValues { get; set; }

        [Key(13)]
        [CopyReference]
        public SelectQuery SelectQuery { get; set; }

        [Key(14)]
        [CopyReference]
        public ChartConfig ChartConfig { get; set; }

        [Key(15)]
        public bool IsShared { get; set; }

        [Key(16)]
        public ICollection<DexihViewParameter> Parameters { get; set; }

        
        public override void ResetKeys()
        {
            Key = 0;
            
            foreach (var parameter in Parameters)
            {
                parameter.ResetKeys();
            }
        }
    }

}