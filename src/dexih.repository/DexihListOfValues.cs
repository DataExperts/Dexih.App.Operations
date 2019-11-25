using System;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;
using MessagePack;
using Newtonsoft.Json;

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
        public long? KeyColumnKey { get; set; }
        
        [Key(13)]
        public long? NameColumnKey { get; set; }
        
        [Key(14)]
        public long? DescriptionColumnKey { get; set; }

        [Key(15)]
        public string StaticData { get; set; }

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