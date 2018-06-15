using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
  public partial class DexihCustomFunctionParameter : DexihParameterBase
  {

    [CopyCollectionKey((long)0, true)]
    public long CustomFunctionParameterKey { get; set; }
    
    [CopyParentCollectionKey]
    public long CustomFunctionKey { get; set; }
      
      // public string Name { get; set; }
      public string Description { get; set; }

    
     [JsonIgnore, CopyIgnore]
     public virtual DexihCustomFunction CustomFunction { get; set; }

  }
}
