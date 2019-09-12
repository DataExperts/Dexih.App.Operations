using System;
using Dexih.Utils.DataType;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class InputColumn
    {
        [ProtoMember(1)]
        public long DatalinkKey { get; set; }

        [ProtoMember(2)]
        public string DatalinkName { get; set; }

        [ProtoMember(3)]
        public string Name { get; set; }

        [ProtoMember(4)]
        public string LogicalName { get; set; }

        [ProtoMember(5)]
        public DataType.ETypeCode DataType { get; set; }

        [ProtoMember(6)]
        public int Rank { get; set; }

        private object _value;

        [ProtoMember(7)]
        public object Value
        {
            get => _value ?? DefaultValue;
            set => _value = value;
        }

        [ProtoMember(8)]
        public object DefaultValue { get; set; }
    }
}