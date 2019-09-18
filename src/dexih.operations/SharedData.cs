using System;
using System.Collections.Generic;
using dexih.functions.Query;
using dexih.repository;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using MessagePack;

namespace dexih.operations
{

    [MessagePackObject]
    public class SharedData
    {
        [Key(0)]
        public long HubKey { get; set; }

        [Key(1)]
        public string HubName { get; set; }

        [Key(2)]
        public EDataObjectType ObjectType { get; set; }

        [Key(3)]
        public long ObjectKey { get; set; }

        [Key(4)]
        public string Name { get; set; }

        [Key(5)]
        public string LogicalName { get; set; }

        [Key(6)]
        public string Description { get; set; }

        [Key(7)]
        public DateTime UpdateDate { get; set; }

        [Key(8)]
        public InputColumn[] InputColumns { get; set; }

        [Key(9)]
        public IEnumerable<InputParameterBase> Parameters { get; set; }

        [Key(10)]
        public SelectQuery Query { get; set; }

        [Key(11)]
        public DexihColumnBase[] OutputColumns { get; set; }
    }
}