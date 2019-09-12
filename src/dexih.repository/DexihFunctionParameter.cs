using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public partial class DexihFunctionParameter : DexihFunctionParameterBase
    {
        public DexihFunctionParameter()
        {
            ArrayParameters = new HashSet<DexihFunctionArrayParameter>();
        }

        [ProtoMember(1)]
        [CopyParentCollectionKey]
		public long DatalinkTransformItemKey { get; set; }

        [ProtoMember(2)]
        public virtual ICollection<DexihFunctionArrayParameter> ArrayParameters { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalinkTransformItem DtItem { get; set; }

    }
}
