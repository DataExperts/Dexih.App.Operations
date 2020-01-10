using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;


namespace dexih.repository
{
    [DataContract]
    public class DexihFunctionArrayParameter : DexihFunctionParameterBase
    {


        /// <summary>
        /// Points to the parent parameter, when it is an array.
        /// </summary>
        [DataMember(Order = 18)]
        [CopyParentCollectionKey]
        public long FunctionParameterKey { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember] public virtual DexihFunctionParameter FunctionParameter { get; set; }

    }
}
