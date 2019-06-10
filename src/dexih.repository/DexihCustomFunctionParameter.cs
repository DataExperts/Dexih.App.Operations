﻿using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
  public partial class DexihCustomFunctionParameter : DexihParameterBase
  {
   
    [CopyParentCollectionKey(nameof(DexihCustomFunction.Key))]
    public long CustomFunctionKey { get; set; }
      
    
     [JsonIgnore, CopyIgnore]
     public virtual DexihCustomFunction CustomFunction { get; set; }

  }
}
