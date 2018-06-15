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
    public partial class DexihColumnValidation : DexihBaseEntity
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
        
        [NotMapped]
        public ETypeCode DataType { get; set; }
        [JsonIgnore, CopyIgnore]
        public string DataTypeString 
        {
            get => DataType.ToString();
            set => DataType = (ETypeCode)Enum.Parse(typeof(ETypeCode), value);
        }
        public int? MinLength { get; set; }
        public int? MaxLength { get; set; }
        public bool AllowDbNull { get; set; }
        public decimal? MinValue { get; set; }
        public decimal? MaxValue { get; set; }
        public string PatternMatch { get; set; }
        public string RegexMatch { get; set; }
        
        [NotMapped, CopyIgnore]
        public string[] ListOfValues { get; set; }
        
        [JsonIgnore]
        public string ListOfValuesString
        {
            get => ListOfValues == null ? null : string.Join("||", ListOfValues);
            set => ListOfValues = string.IsNullOrEmpty(value) ? null : value.Split(new[] { "||" }, StringSplitOptions.None).ToArray();
        }
        
        [NotMapped, CopyIgnore]
        public string[] ListOfNotValues { get; set; }

        [JsonIgnore]
        public string ListOfNotValuesString
        {
            get => ListOfNotValues == null ? null : string.Join("||", ListOfNotValues);
            set => ListOfNotValues = string.IsNullOrEmpty(value) ? null : value.Split(new[] { "||" }, StringSplitOptions.None).ToArray();
        }
        
        [JsonIgnore, CopyIgnore]
        public virtual DexihTableColumn LookupColumn { get; set; }


        public long? LookupColumnKey { get; set; }
        public bool LookupIsValid { get; set; }
        public bool LookupMultipleRecords { get; set; }

        [NotMapped]
        public TransformFunction.EInvalidAction InvalidAction { get; set; }
        [JsonIgnore, CopyIgnore]
        public string InvalidActionString {
            get => InvalidAction.ToString();
            set => InvalidAction = (TransformFunction.EInvalidAction)Enum.Parse(typeof(TransformFunction.EInvalidAction), value);
        }        
        [NotMapped]
        public ECleanAction CleanAction { get; set; }
        [JsonIgnore, CopyIgnore]
        public string CleanActionString{
            get => CleanAction.ToString();
            set => CleanAction = (ECleanAction)Enum.Parse(typeof(ECleanAction), value);
        }
        public string CleanValue { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihHub Hub { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihTableColumn> DexihColumnValidationColumn {get; set;}
        
    }
}
