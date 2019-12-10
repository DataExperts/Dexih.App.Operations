using System.Collections.Generic;
using System.Text.Json.Serialization;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihListOfValues : DexihHubNamedEntity
    {
        [Key(7)]
        public ELOVObjectType SourceType { get; set; }

        [Key(8)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [Key(9)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }

        [CopyReference]
        [Key(11)]
        public SelectQuery SelectQuery { get; set; }
        
        [Key(12)]
        public string KeyColumn { get; set; }
        
        [Key(13)]
        public string NameColumn { get; set; }
        
        [Key(14)]
        public string DescriptionColumn { get; set; }

        [Key(15), CopyReference]
        public ICollection<ListOfValuesItem> StaticData { get; set; }

        [Key(16)] 
        public bool Cache { get; set; }
        
        [Key(17)] 
        public int CacheSeconds { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihDatalink SourceDatalink { get; set; }

        public override void ResetKeys()
        {
            Key = 0;
        }
        
    }
    
    
}