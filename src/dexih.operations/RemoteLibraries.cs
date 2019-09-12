using System;
using System.Collections.Generic;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.Transforms;
using ProtoBuf;

namespace dexih.operations
{
    [ProtoContract]
    public class RemoteLibraries
    {
        [ProtoMember(1)]
        public List<FunctionReference> Functions { get; set; }

        [ProtoMember(2)]
        public List<ConnectionReference> Connections { get; set; }

        [ProtoMember(3)]
        public List<TransformReference> Transforms { get; set; }
    }
}