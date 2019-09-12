using System;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihCustomFunctionParameter : DexihParameterBase
    {
        [ProtoMember(1)]
        [CopyParentCollectionKey(nameof(Key))]
        public long CustomFunctionKey { get; set; }


        [JsonIgnore, CopyIgnore]
        public virtual DexihCustomFunction CustomFunction { get; set; }

    }
}
