using System.Runtime.Serialization;
using Dexih.Utils.DataType;


namespace dexih.repository
{
    [DataContract]
    public class InputColumn
    {
        [DataMember(Order = 0)]
        public long DatalinkKey { get; set; }

        [DataMember(Order = 1)]
        public string DatalinkName { get; set; }

        [DataMember(Order = 2)]
        public string Name { get; set; }

        [DataMember(Order = 3)]
        public string LogicalName { get; set; }

        [DataMember(Order = 4)]
        public ETypeCode DataType { get; set; }

        [DataMember(Order = 5)]
        public int Rank { get; set; }

        private object _value;

        [DataMember(Order = 6)]
        public object Value
        {
            get => _value ?? DefaultValue;
            set => _value = value;
        }

        [DataMember(Order = 7)]
        public object DefaultValue { get; set; }
    }
}