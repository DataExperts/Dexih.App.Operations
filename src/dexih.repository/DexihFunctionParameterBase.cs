using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [Serializable]
    public class DexihFunctionParameterBase: DexihParameterBase
    {
        // [JsonIgnore]
        public long? DatalinkColumnKey { get; set; }
        
        public string Value { get; set; }
        
        public List<string> ListOfValues { get; set; }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }
        
        [CopyIgnore]
        public DexihDatalinkColumn DatalinkColumn { get; set; }
    }
}