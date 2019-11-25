namespace dexih.repository
{
    public class ListOfValueItem<T>
    {
        public ListOfValueItem(T key, string name, string description)
        {
            Key = key;
            Name = name;
            Description = description;
        }
        public T Key { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
    }
    
}