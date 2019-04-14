using Dexih.Utils.DataType;

namespace dexih.repository
{
    public class InputColumn
    {
        public long DatalinkKey { get; set; }
        public string DatalinkName { get; set; }
        
        public string Name { get; set; }
        public string LogicalName { get; set; }
        public DataType.ETypeCode DataType { get; set; }
        public int Rank { get; set; }

        private object _value;

        public object Value
        {
            get => _value ?? DefaultValue;
            set => _value = value;
        } 

        public object DefaultValue { get; set; }
    }
}