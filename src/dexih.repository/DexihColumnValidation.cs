using System;
using System.Collections.Generic;
using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihColumnValidation : DexihHubNamedEntity
    {

        public DexihColumnValidation()
        {
            DexihColumnValidationColumn = new HashSet<DexihTableColumn>();
        }

        [ProtoMember(1)]
        public ETypeCode DataType { get; set; }

        [ProtoMember(2)]
        public int? MinLength { get; set; }

        [ProtoMember(3)]
        public int? MaxLength { get; set; }

        [ProtoMember(4)]
        public bool AllowDbNull { get; set; }

        [ProtoMember(5)]
        public decimal? MinValue { get; set; }

        [ProtoMember(6)]
        public decimal? MaxValue { get; set; }

        [ProtoMember(7)]
        public string PatternMatch { get; set; }

        [ProtoMember(8)]
        public string RegexMatch { get; set; }

        [ProtoMember(9)]
        [CopyIgnore]
        public List<string> ListOfValues { get; set; }

        [ProtoMember(10)]
        [CopyIgnore]
        public List<string> ListOfNotValues { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihTableColumn LookupColumn { get; set; }


        [ProtoMember(11)]
        public long? LookupColumnKey { get; set; }

        [ProtoMember(12)]
        public bool LookupIsValid { get; set; }

        [ProtoMember(13)]
        public bool LookupMultipleRecords { get; set; }

        [ProtoMember(14)]
        public TransformFunction.EInvalidAction InvalidAction { get; set; }

        [ProtoMember(15)]
        public ECleanAction CleanAction { get; set; }

        [ProtoMember(16)]
        public string CleanValue { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihTableColumn> DexihColumnValidationColumn {get; set;}
        
    }
}
