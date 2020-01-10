using System.Collections.Generic;
using System.Runtime.Serialization;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.Transforms;


namespace dexih.operations
{
    [DataContract]
    public class RemoteLibraries
    {
        [DataMember(Order = 0)]
        public List<FunctionReference> Functions { get; set; }

        [DataMember(Order = 1)]
        public List<ConnectionReference> Connections { get; set; }

        [DataMember(Order = 2)]
        public List<TransformReference> Transforms { get; set; }
    }
}