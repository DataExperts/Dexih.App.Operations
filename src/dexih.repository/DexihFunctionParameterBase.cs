using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihFunctionParameterBase: DexihParameterBase
    {
        [JsonIgnore]
        public long? DatalinkColumnKey { get; set; }
        
        public string Value { get; set; }
        
        public string[] ListOfValues { get; set; }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }
        
        [CopyIgnore]
        public DexihDatalinkColumn DatalinkColumn { get; set; }
    }
}