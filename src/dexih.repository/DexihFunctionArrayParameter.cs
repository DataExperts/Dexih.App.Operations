using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
  public partial class DexihFunctionArrayParameter : DexihFunctionParameterBase
  {

    [CopyCollectionKey((long)0, true)]
    public long FunctionArrayParameterKey { get; set; }
    
    /// <summary>
    /// Points to the parent parameter, when it is an array.
    /// </summary>
    [CopyParentCollectionKey]
    public long FunctionParameterKey { get; set; }

    [JsonIgnore, CopyIgnore]
    public virtual DexihFunctionParameter FunctionParameter { get; set; }

  }
}
