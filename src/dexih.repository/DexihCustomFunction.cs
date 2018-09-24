using System;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;

namespace dexih.repository
{
    public class DexihCustomFunction : DexihBaseEntity
    {

        public DexihCustomFunction()
        {
            DexihDatalinkTransformItemCustomFunction = new HashSet<DexihDatalinkTransformItem>();
            DexihCustomFunctionParameters = new HashSet<DexihCustomFunctionParameter>();
        }

        [CopyCollectionKey((long)0, true)]
        public long CustomFunctionKey { get; set; }
        public long HubKey { get; set; }
        public string MethodCode { get; set; }
        public string ResultCode { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public DataType.ETypeCode? ReturnType { get; set; }
        public EFunctionType? FunctionType { get; set; }

        public ICollection<DexihCustomFunctionParameter> DexihCustomFunctionParameters { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemCustomFunction { get; set; }

    }
}
