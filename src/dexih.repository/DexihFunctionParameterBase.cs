using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using Newtonsoft.Json;

namespace dexih.repository
{
    public class DexihFunctionParameterBase: DexihParameterBase
    {
        public long? DatalinkColumnKey { get; set; }
        
        public string Value { get; set; }
        
        public string[] ListOfValues { get; set; }
        
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }
        
        public virtual DexihDatalinkColumn DatalinkColumn { get; set; }

    }
}