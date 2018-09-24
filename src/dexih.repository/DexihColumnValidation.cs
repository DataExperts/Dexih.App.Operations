using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using dexih.functions;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using static Dexih.Utils.DataType.DataType;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public class DexihColumnValidation : DexihBaseEntity
    {
        [JsonConverter(typeof(StringEnumConverter))]
        public enum ECleanAction
        {
            DefaultValue = 1, Truncate = 2, Blank = 3, Null = 4, OriginalValue = 5, CleanValue = 6 //action when clean is required.
        }

        public DexihColumnValidation()
        {
            DexihColumnValidationColumn = new HashSet<DexihTableColumn>();
        }

        [CopyCollectionKey((long)0, true)]
        public long ColumnValidationKey { get; set; }

        [JsonIgnore, CopyIgnore]
        public long HubKey { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        
        public ETypeCode DataType { get; set; }
        public int? MinLength { get; set; }
        public int? MaxLength { get; set; }
        public bool AllowDbNull { get; set; }
        public decimal? MinValue { get; set; }
        public decimal? MaxValue { get; set; }
        public string PatternMatch { get; set; }
        public string RegexMatch { get; set; }
        
        [CopyIgnore]
        public string[] ListOfValues { get; set; }
        
        [CopyIgnore]
        public string[] ListOfNotValues { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihTableColumn LookupColumn { get; set; }


        public long? LookupColumnKey { get; set; }
        public bool LookupIsValid { get; set; }
        public bool LookupMultipleRecords { get; set; }

        public TransformFunction.EInvalidAction InvalidAction { get; set; }
        public ECleanAction CleanAction { get; set; }

        public string CleanValue { get; set; }

        [JsonIgnore, CopyIgnore]
        public DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihTableColumn> DexihColumnValidationColumn {get; set;}
        
    }
}
