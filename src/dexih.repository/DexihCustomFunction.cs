using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;


namespace dexih.repository
{
    [DataContract]
    public class DexihCustomFunction : DexihHubNamedEntity
    {

        public DexihCustomFunction()
        {
            DexihDatalinkTransformItemCustomFunction = new HashSet<DexihDatalinkTransformItem>();
            DexihCustomFunctionParameters = new HashSet<DexihCustomFunctionParameter>();
        }

        [DataMember(Order = 7)]
        public string MethodCode { get; set; }

        [DataMember(Order = 8)]
        public string ResultCode { get; set; }

        [DataMember(Order = 9)]
        public ETypeCode? ReturnType { get; set; }

        [DataMember(Order = 10)]
        public EFunctionType? FunctionType { get; set; }

        [DataMember(Order = 11)]
        public bool IsGeneric { get; set; }

        [DataMember(Order = 12)]
        public ETypeCode GenericTypeDefault { get; set; }

        [DataMember(Order = 13)]
        public ICollection<DexihCustomFunctionParameter> DexihCustomFunctionParameters { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemCustomFunction { get; set; }

        public override void ResetKeys()
        {
            base.ResetKeys();
            
            foreach (var parameter in DexihCustomFunctionParameters)
            {
                parameter.ResetKeys();
            }
        }
    }
}
