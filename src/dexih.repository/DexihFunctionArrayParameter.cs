using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public partial class DexihFunctionArrayParameter : DexihFunctionParameterBase
    {


        /// <summary>
        /// Points to the parent parameter, when it is an array.
        /// </summary>
        [Key(18)]
        [CopyParentCollectionKey]
        public long FunctionParameterKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember] public virtual DexihFunctionParameter FunctionParameter { get; set; }

    }
}
