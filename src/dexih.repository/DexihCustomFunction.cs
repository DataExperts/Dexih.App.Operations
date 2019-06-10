using Newtonsoft.Json;
using System.Collections.Generic;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;

namespace dexih.repository
{
    public class DexihCustomFunction : DexihHubNamedEntity
    {

        public DexihCustomFunction()
        {
            DexihDatalinkTransformItemCustomFunction = new HashSet<DexihDatalinkTransformItem>();
            DexihCustomFunctionParameters = new HashSet<DexihCustomFunctionParameter>();
        }

        public string MethodCode { get; set; }
        public string ResultCode { get; set; }
        public DataType.ETypeCode? ReturnType { get; set; }
        public EFunctionType? FunctionType { get; set; }
        public bool IsGeneric { get; set; }
        public DataType.ETypeCode GenericTypeDefault { get; set; }


        public ICollection<DexihCustomFunctionParameter> DexihCustomFunctionParameters { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemCustomFunction { get; set; }

    }
}
