using System;
using Newtonsoft.Json;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [Serializable]
    public partial class DexihDatalinkProfile : DexihHubNamedEntity
    {


		[CopyParentCollectionKey]
        public long DatalinkKey { get; set; }
        
        public string FunctionClassName { get; set; }
        public string FunctionAssemblyName { get; set; }
        public string FunctionMethodName { get; set; }

        public bool DetailedResults { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual DexihDatalink Datalink { get; set; }
    }
}
