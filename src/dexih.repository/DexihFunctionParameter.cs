using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    public class DexihFunctionParameter : DexihFunctionParameterBase
    {
        public DexihFunctionParameter()
        {
            ArrayParameters = new HashSet<DexihFunctionArrayParameter>();
        }

        [DataMember(Order = 18)]
        [CopyParentCollectionKey]
		public long DatalinkTransformItemKey { get; set; }

        [DataMember(Order = 19)]
        public virtual ICollection<DexihFunctionArrayParameter> ArrayParameters { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public virtual DexihDatalinkTransformItem DtItem { get; set; }

    }
}
