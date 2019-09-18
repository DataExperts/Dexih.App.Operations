using System;
using System.Collections.Generic;
using dexih.functions;
using dexih.transforms;
using dexih.transforms.Transforms;
using MessagePack;

namespace dexih.operations
{
    [MessagePackObject]
    public class RemoteLibraries
    {
        [Key(0)]
        public List<FunctionReference> Functions { get; set; }

        [Key(1)]
        public List<ConnectionReference> Connections { get; set; }

        [Key(2)]
        public List<TransformReference> Transforms { get; set; }
    }
}