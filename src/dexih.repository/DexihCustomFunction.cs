using System;
using Newtonsoft.Json;
using System.Collections.Generic;
using dexih.functions;
using Dexih.Utils.CopyProperties;
using Dexih.Utils.DataType;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class DexihCustomFunction : DexihHubNamedEntity
    {

        public DexihCustomFunction()
        {
            DexihDatalinkTransformItemCustomFunction = new HashSet<DexihDatalinkTransformItem>();
            DexihCustomFunctionParameters = new HashSet<DexihCustomFunctionParameter>();
        }

        [ProtoMember(1)]
        public string MethodCode { get; set; }

        [ProtoMember(2)]
        public string ResultCode { get; set; }

        [ProtoMember(3)]
        public DataType.ETypeCode? ReturnType { get; set; }

        [ProtoMember(4)]
        public EFunctionType? FunctionType { get; set; }

        [ProtoMember(5)]
        public bool IsGeneric { get; set; }

        [ProtoMember(6)]
        public DataType.ETypeCode GenericTypeDefault { get; set; }

        [ProtoMember(7)]
        public ICollection<DexihCustomFunctionParameter> DexihCustomFunctionParameters { get; set; }

        [JsonIgnore, CopyIgnore]
        public ICollection<DexihDatalinkTransformItem> DexihDatalinkTransformItemCustomFunction { get; set; }

    }
}
