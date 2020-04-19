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

        private long? _sourceTableKey;
        private long? _sourceDatalinkKey;

        [DataMember(Order = 8)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey {
            get => SourceType == ELOVObjectType.Table ? _sourceTableKey : null;
            set => _sourceTableKey = value;
        }

        [DataMember(Order = 9)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey
        {
            get => SourceType == ELOVObjectType.Datalink ? _sourceDatalinkKey : null;
            set => _sourceDatalinkKey = value;
        }

        [CopyReference]
        [DataMember(Order = 10)]
        public SelectQuery SelectQuery { get; set; }
        
        [DataMember(Order = 11)]
        public string KeyColumn { get; set; }
        
        [DataMember(Order = 12)]
        public string NameColumn { get; set; }
        
        [DataMember(Order = 13)]
        public string DescriptionColumn { get; set; }

        [DataMember(Order = 14), CopyReference]
        public ICollection<ListOfValuesItem> StaticData { get; set; }

        [DataMember(Order = 15)] 
        public bool Cache { get; set; }
        
        [DataMember(Order = 16)] 
        public int CacheSeconds { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalink SourceDatalink { get; set; }
        
    }
    
    
}