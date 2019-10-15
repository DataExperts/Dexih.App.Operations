using System.Collections.Generic;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class DexihCustomFunction : DexihHubNamedEntity
    {

        public DexihCustomFunction()
        {
            DexihDatalinkTransformItemCustomFunction = new HashSet<DexihDatalinkTransformItem>();
            DexihCustomFunctionParameters = new HashSet<DexihCustomFunctionParameter>();
        }

        [Key(7)]
        public string MethodCode { get; set; }

        [Key(8)]
        public string ResultCode { get; set; }

        [Key(9)]
        public ETypeCode? ReturnType { get; set; }

        [Key(10)]
        public EFunctionType? FunctionType { get; set; }

        [Key(11)]
        public bool IsGeneric { get; set; }

        [Key(12)]
        public ETypeCode GenericTypeDefault { get; set; }

        [Key(13)]
        public ICollection<DexihCustomFunctionParameter> DexihCustomFunctionParameters { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreMember]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemCustomFunction { get; set; }

    }
}
