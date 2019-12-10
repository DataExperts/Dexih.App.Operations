using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihColumnValidation : DexihHubNamedEntity
    {

        public DexihColumnValidation()
        {
            DexihColumnValidationColumn = new HashSet<DexihTableColumn>();
        }

        [Key(7)]
        public ETypeCode DataType { get; set; }

        [Key(8)]
        public int? MinLength { get; set; }

        [Key(9)]
        public int? MaxLength { get; set; }

        [Key(10)]
        public bool AllowDbNull { get; set; }

        [Key(11)]
        public decimal? MinValue { get; set; }

        [Key(12)]
        public decimal? MaxValue { get; set; }

        [Key(13)]
        public string PatternMatch { get; set; }

        [Key(14)]
        public string RegexMatch { get; set; }
        
        [Key(15)]
        [NotMapped, CopyIgnore]
        public string[] ListOfValues
        {
            get
            {
                if (string.IsNullOrEmpty(ListOfValuesString))
                {
                    return null;
                }
                else
                {
                    return ListOfValuesString.Deserialize<string[]>(true);
                }
            }
            set => ListOfValuesString = value.Serialize();
        }
        
        [JsonIgnore, IgnoreMember]
        public string ListOfValuesString { get; set; }
        
        [Key(16)]
        [NotMapped, CopyIgnore]
        public string[] ListOfNotValues
        {
            get
            {
                if (string.IsNullOrEmpty(ListOfNotValuesString))
                {
                    return null;
                }
                else
                {
                    return ListOfNotValuesString.Deserialize<string[]>(true);
                }
            }
            set => ListOfNotValuesString = value.Serialize();
        }
        
        [JsonIgnore, IgnoreMember]
        public string ListOfNotValuesString { get; set; }



        [JsonIgnore, CopyIgnore, IgnoreMember]
        public DexihTableColumn LookupColumn { get; set; }


        [Key(17)]
        public long? LookupColumnKey { get; set; }

        [Key(18)]
        public bool LookupIsValid { get; set; }

        [Key(19)]
        public bool LookupMultipleRecords { get; set; }

        [Key(20)]
        public TransformFunction.EInvalidAction InvalidAction { get; set; }

        [Key(21)]
        public ECleanAction CleanAction { get; set; }

        [Key(22)]
        public string CleanValue { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreMember]
        public ICollection<DexihTableColumn> DexihColumnValidationColumn {get; set;}
        
        public override void ResetKeys()
        {
            Key = 0;
            
            foreach (var column in DexihColumnValidationColumn)
            {
                column.ResetKeys();
            }
        }
    }
}
