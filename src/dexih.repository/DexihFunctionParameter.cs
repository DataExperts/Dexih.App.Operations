using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    public partial class DexihFunctionParameter : DexihParameterBase
    {

        [CopyCollectionKey((long)0, true)]
		public long FunctionParameterKey { get; set; }
		
        [CopyParentCollectionKey]
		public long DatalinkTransformItemKey { get; set; }

        public long? DatalinkColumnKey { get; set; }
        
        public string Value { get; set; }

        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkTransformItem DtItem { get; set; }

        [CopyReference]
        public virtual DexihDatalinkColumn DatalinkColumn { get; set; }
    }
}
