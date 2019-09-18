using System;
using Dexih.Utils.DataType;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class InputColumn
    {
        [Key(0)]
        public long DatalinkKey { get; set; }

        [Key(1)]
        public string DatalinkName { get; set; }

        [Key(2)]
        public string Name { get; set; }

        [Key(3)]
        public string LogicalName { get; set; }

        [Key(4)]
        public DataType.ETypeCode DataType { get; set; }

        [Key(5)]
        public int Rank { get; set; }

        private object _value;

        [Key(6)]
        public object Value
        {
            get => _value ?? DefaultValue;
            set => _value = value;
        }

        [Key(7)]
        public object DefaultValue { get; set; }
    }
}