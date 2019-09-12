using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using Dexih.Utils.CopyProperties;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    [ProtoInclude(100, typeof(DexihFunctionArrayParameter))]
    [ProtoInclude(200, typeof(DexihFunctionParameter))]
    public class DexihFunctionParameterBase: DexihParameterBase
    {
        [ProtoMember(1)]
        public long? DatalinkColumnKey { get; set; }

        [ProtoMember(2)]
        public string Value { get; set; }

        [ProtoMember(3)]
        public List<string> ListOfValues { get; set; }

        [ProtoMember(4)]
        [NotMapped]
        public EntityStatus EntityStatus { get; set; }

        [ProtoMember(5)]
        [CopyIgnore]
        public DexihDatalinkColumn DatalinkColumn { get; set; }
    }
}