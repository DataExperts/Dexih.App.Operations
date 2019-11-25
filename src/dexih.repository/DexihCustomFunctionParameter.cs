using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihCustomFunctionParameter : DexihParameterBase
    {
        [Key(13)]
        [CopyParentCollectionKey(nameof(Key))]
        public long CustomFunctionKey { get; set; }


        [JsonIgnore, CopyIgnore, IgnoreMember]
        public virtual DexihCustomFunction CustomFunction { get; set; }

    }
}
