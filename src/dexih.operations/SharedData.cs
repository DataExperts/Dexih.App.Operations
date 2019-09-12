using System;
using System.Collections.Generic;
using dexih.functions.Query;
using dexih.repository;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;

namespace dexih.operations
{

    [ProtoContract]
    public class SharedData
    {
        [ProtoMember(1)]
        public long HubKey { get; set; }

        [ProtoMember(2)]
        public string HubName { get; set; }

        [ProtoMember(3)]
        public EDataObjectType ObjectType { get; set; }

        [ProtoMember(4)]
        public long ObjectKey { get; set; }

        [ProtoMember(5)]
        public string Name { get; set; }

        [ProtoMember(6)]
        public string LogicalName { get; set; }

        [ProtoMember(7)]
        public string Description { get; set; }

        [ProtoMember(8)]
        public DateTime UpdateDate { get; set; }

        [ProtoMember(9)]
        public InputColumn[] InputColumns { get; set; }

        [ProtoMember(10)]
        public IEnumerable<InputParameterBase> Parameters { get; set; }

        [ProtoMember(11)]
        public SelectQuery Query { get; set; }

        [ProtoMember(12)]
        public DexihColumnBase[] OutputColumns { get; set; }
    }
}