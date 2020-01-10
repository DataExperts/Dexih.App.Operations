using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    public class DexihListOfValues : DexihHubNamedEntity
    {
        [DataMember(Order = 7)]
        public ELOVObjectType SourceType { get; set; }

        [DataMember(Order = 8)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [DataMember(Order = 9)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }

        [CopyReference]
        [DataMember(Order = 11)]
        public SelectQuery SelectQuery { get; set; }
        
        [DataMember(Order = 12)]
        public string KeyColumn { get; set; }
        
        [DataMember(Order = 13)]
        public string NameColumn { get; set; }
        
        [DataMember(Order = 14)]
        public string DescriptionColumn { get; set; }

        [DataMember(Order = 15), CopyReference]
        public ICollection<ListOfValuesItem> StaticData { get; set; }

        [DataMember(Order = 16)] 
        public bool Cache { get; set; }
        
        [DataMember(Order = 17)] 
        public int CacheSeconds { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalink SourceDatalink { get; set; }

        public override void ResetKeys()
        {
            Key = 0;
        }
        
    }
    
    
}