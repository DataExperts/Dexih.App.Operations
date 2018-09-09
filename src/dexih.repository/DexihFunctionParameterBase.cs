using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihFunctionParameterBase: DexihParameterBase
    {
        public long? DatalinkColumnKey { get; set; }
        
        public string Value { get; set; }
        
        [NotMapped]
        public string[] ListOfValues { get; set; }
        
        [JsonIgnore, CopyIgnore]
        public string ListOfValuesString
        {
            get => ListOfValues == null ? null : string.Join(",", ListOfValues);
            set => ListOfValues = value?.Split(',');
        }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }
        
        public virtual DexihDatalinkColumn DatalinkColumn { get; set; }

    }
}