using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;


namespace dexih.repository
{
    [DataContract]
    public class DexihColumnValidation : DexihHubNamedEntity
    {

        public DexihColumnValidation()
        {
            DexihColumnValidationColumn = new HashSet<DexihTableColumn>();
        }

        [DataMember(Order = 7)]
        public ETypeCode DataType { get; set; }

        [DataMember(Order = 8)]
        public int? MinLength { get; set; }

        [DataMember(Order = 9)]
        public int? MaxLength { get; set; }

        [DataMember(Order = 10)]
        public bool AllowDbNull { get; set; }

        [DataMember(Order = 11)]
        public decimal? MinValue { get; set; }

        [DataMember(Order = 12)]
        public decimal? MaxValue { get; set; }

        [DataMember(Order = 13)]
        public string PatternMatch { get; set; }

        [DataMember(Order = 14)]
        public string RegexMatch { get; set; }
        
        [DataMember(Order = 15)]
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
        
        [JsonIgnore, IgnoreDataMember]
        public string ListOfValuesString { get; set; }
        
        [DataMember(Order = 16)]
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
        
        [JsonIgnore, IgnoreDataMember]
        public string ListOfNotValuesString { get; set; }



        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public DexihTableColumn LookupColumn { get; set; }


        [DataMember(Order = 17)]
        public long? LookupColumnKey { get; set; }

        [DataMember(Order = 18)]
        public bool LookupIsValid { get; set; }

        [DataMember(Order = 19)]
        public bool LookupMultipleRecords { get; set; }

        [DataMember(Order = 20)]
        public TransformFunction.EInvalidAction InvalidAction { get; set; }

        [DataMember(Order = 21)]
        public ECleanAction CleanAction { get; set; }

        [DataMember(Order = 22)]
        public string CleanValue { get; set; }
        
        [JsonIgnore, CopyIgnore, IgnoreDataMember]
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
