using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
  public partial class DexihFunctionArrayParameter : DexihFunctionParameterBase
  {

   
    /// <summary>
    /// Points to the parent parameter, when it is an array.
    /// </summary>
    [CopyParentCollectionKey]
    public long FunctionParameterKey { get; set; }

    [JsonIgnore, CopyIgnore]
    public virtual DexihFunctionParameter FunctionParameter { get; set; }

  }
}
