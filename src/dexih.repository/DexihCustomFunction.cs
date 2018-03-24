using System;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using dexih.functions;
using dexih.functions.Query;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;
using Newtonsoft.Json.Converters;

namespace dexih.repository
{
    public partial class DexihCustomFunction : DexihBaseEntity
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
        
        [NotMapped]
        public DataType.ETypeCode? ReturnType { get; set; }

        [JsonIgnore, CopyIgnore]
        public string ReturnTypeString
        {
            get => ReturnType.ToString();
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    ReturnType = null;
                }
                else
                {
                    ReturnType = (DataType.ETypeCode)Enum.Parse(typeof(DataType.ETypeCode), value);
                }
            }
        }

        [NotMapped]
        public EFunctionType? FunctionType { get; set; }

        [JsonIgnore, CopyIgnore]
        public string FunctionTypeString
        {
            get => FunctionType.ToString();
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    FunctionType = null;
                }
                else
                {
                    FunctionType = (EFunctionType)Enum.Parse(typeof(EFunctionType), value);
                }
            }
        }

        public virtual ICollection<DexihCustomFunctionParameter> DexihCustomFunctionParameters { get; set; }

        [JsonIgnore, CopyIgnore]
        public virtual ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemCustomFunction { get; set; }

    }
}
