using System;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [Serializable]
  public class DexihCustomFunctionParameter : DexihParameterBase
  {
   
    [CopyParentCollectionKey(nameof(Key))]
    public long CustomFunctionKey { get; set; }
      
    
     [JsonIgnore, CopyIgnore]
     public virtual DexihCustomFunction CustomFunction { get; set; }

  }
}
