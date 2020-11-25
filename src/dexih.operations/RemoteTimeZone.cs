using System.Runtime.Serialization;

namespace dexih.operations
{
    [DataContract]
    public class RemoteTimeZone
    {
        [DataMember(Order = 0)]
        public string Name { get; set; }
        
        [DataMember(Order = 1)]
        public string Description { get; set; }
    }
}