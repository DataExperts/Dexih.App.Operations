using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
  public partial class DexihCustomFunctionParameter : DexihParameterBase
  {

    [CopyCollectionKey((long)0, true)]
    public long CustomFunctionParameterKey { get; set; }
    
    [CopyParentCollectionKey]
    public long CustomFunctionKey { get; set; }
      
      public string Name { get; set; }
      public string Description { get; set; }

    
     [JsonIgnore, CopyIgnore]
     public virtual DexihCustomFunction CustomFunction { get; set; }

  }
}
