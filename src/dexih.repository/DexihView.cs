using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;




namespace dexih.repository
{
    [DataContract]
    public class DexihView : DexihHubNamedEntity
    {
        public DexihView()
        {
            Parameters = new HashSet<DexihViewParameter>();
        }
        
        [DataMember(Order = 7)] 
        public EViewType ViewType { get; set; } = EViewType.Table;

        private long? _sourceTableKey;
        private long? _sourceDatalinkKey;

        [DataMember(Order = 8)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey {
            get => SourceType == EDataObjectType.Table ? _sourceTableKey : null;
            set => _sourceTableKey = value;
        }

        [DataMember(Order = 9)]
        // [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey
        {
            get => SourceType == EDataObjectType.Datalink ? _sourceDatalinkKey : null;
            set => _sourceDatalinkKey = value;
        }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihDatalink SourceDatalink { get; set; }

        [DataMember(Order = 10)]
        public EDataObjectType SourceType { get; set; }

        [DataMember(Order = 11)] public bool AutoRefresh { get; set; } = true;

        [DataMember(Order = 12)]
        [NotMapped]
        [CopyReference]
        public InputColumn[] InputValues { get; set; }

        [DataMember(Order = 13)]
        [CopyReference]
        public SelectQuery SelectQuery { get; set; }

        [DataMember(Order = 14)]
        [CopyReference]
        public ChartConfig ChartConfig { get; set; }

        [DataMember(Order = 15)]
        public bool IsShared { get; set; }

        [DataMember(Order = 16)]
        public ICollection<DexihViewParameter> Parameters { get; set; }

        
        public override void ResetKeys()
        {
            base.ResetKeys();
            
            foreach (var parameter in Parameters)
            {
                parameter.ResetKeys();
            }
        }
    }

}