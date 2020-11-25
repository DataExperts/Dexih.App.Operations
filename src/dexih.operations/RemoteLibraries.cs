using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.Transforms;


namespace dexih.operations
{
    [DataContract]
    public class RemoteLibraries
    {
        public RemoteLibraries()
        {
            TimeZones = TimeZoneInfo.GetSystemTimeZones().Select(c => new RemoteTimeZone()
                {Name = c.Id, Description = c.Id + " " + c.DisplayName}).ToList();
        }
        
        [DataMember(Order = 0)]
        public List<FunctionReference> Functions { get; set; }

        [DataMember(Order = 1)]
        public List<ConnectionReference> Connections { get; set; }

        [DataMember(Order = 2)]
        public List<TransformReference> Transforms { get; set; }
        
        [DataMember(Order = 3)]
        public List<RemoteTimeZone> TimeZones { get; set; }

    }
}