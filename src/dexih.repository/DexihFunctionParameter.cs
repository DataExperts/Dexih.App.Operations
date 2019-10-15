using System.Collections.Generic;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public partial class DexihFunctionParameter : DexihFunctionParameterBase
    {
        public DexihFunctionParameter()
        {
            ArrayParameters = new HashSet<DexihFunctionArrayParameter>();
        }

        [Key(5)]
        [CopyParentCollectionKey]
		public long DatalinkTransformItemKey { get; set; }

        [Key(6)]
        public virtual ICollection<DexihFunctionArrayParameter> ArrayParameters { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihDatalinkTransformItem DtItem { get; set; }

    }
}
