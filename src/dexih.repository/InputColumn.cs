using Dexih.Utils.DataType;

namespace dexih.repository
{
    public class InputColumn
    {
        public string Name { get; set; }
        public string LogicalName { get; set; }
        public DataType.ETypeCode DataType { get; set; }
        public int Rank { get; set; }
        
        public object Value { get; set; }
    }
}