using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    public class DexihCustomFunctionParameter : DexihParameterBase
    {
        [DataMember(Order = 13)]
        [CopyParentCollectionKey(nameof(Key))]
        public long CustomFunctionKey { get; set; }


        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public virtual DexihCustomFunction CustomFunction { get; set; }

    }
}
