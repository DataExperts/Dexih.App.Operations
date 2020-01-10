using System.Runtime.Serialization;


namespace dexih.repository
{
    [DataContract]
    public class ListOfValuesItem
    {
        
        [DataMember(Order = 0)]
        public string Key { get; set; }
        [DataMember(Order = 1)]
        public string Name { get; set; }
        [DataMember(Order = 2)]
        public string Description { get; set; }
    }
    
}