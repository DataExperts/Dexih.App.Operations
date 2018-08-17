using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public class DexihFunctionParameterBase: DexihParameterBase
    {
        public long? DatalinkColumnKey { get; set; }
        
        public string Value { get; set; }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }
        
        public virtual DexihDatalinkColumn DatalinkColumn { get; set; }

    }
}