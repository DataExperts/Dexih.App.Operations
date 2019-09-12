using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihView : DexihHubNamedEntity
    {
        public DexihView()
        {
            Parameters = new HashSet<DexihViewParameter>();
        }



        [ProtoMember(1)]
        public EViewType ViewType { get; set; }

        [ProtoMember(2)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceTableKey { get; set; }

        [ProtoMember(3)]
        [JsonProperty(DefaultValueHandling = DefaultValueHandling.IgnoreAndPopulate)]
        public long? SourceDatalinkKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihTable SourceTable { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihDatalink SourceDatalink { get; set; }

        [ProtoMember(4)]
        public EDataObjectType SourceType { get; set; }

        [ProtoMember(5)]
        public bool AutoRefresh { get; set; }

        [ProtoMember(6)]
        [NotMapped]
        [CopyReference]
        public InputColumn[] InputValues { get; set; }

        [ProtoMember(7)]
        [CopyReference]
        public SelectQuery SelectQuery { get; set; }

        [ProtoMember(8)]
        [CopyReference]
        public ChartConfig ChartConfig { get; set; }

        [ProtoMember(9)]
        public bool IsShared { get; set; }

        [ProtoMember(10)]
        public ICollection<DexihViewParameter> Parameters { get; set; }

    }

}