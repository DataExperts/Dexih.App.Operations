using System.Collections.Generic;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    public partial class DexihFunctionParameter : DexihFunctionParameterBase
    {
        public DexihFunctionParameter()
        {
            ArrayParameters = new HashSet<DexihFunctionArrayParameter>();
        }

		
        [CopyParentCollectionKey]
		public long DatalinkTransformItemKey { get; set; }

        public virtual ICollection<DexihFunctionArrayParameter> ArrayParameters { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkTransformItem DtItem { get; set; }

    }
}
