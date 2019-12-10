using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class ListOfValuesItem
    {
        
        [Key(0)]
        public string Key { get; set; }
        [Key(1)]
        public string Name { get; set; }
        [Key(2)]
        public string Description { get; set; }
    }
    
}