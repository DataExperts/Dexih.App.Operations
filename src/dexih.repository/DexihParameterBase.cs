﻿using dexih.functions;
using Dexih.Utils.DataType;
using MessagePack;
using static Dexih.Utils.DataType.DataType;

namespace dexih.repository
{
    [MessagePackObject]
    [Union(0, typeof(DexihCustomFunctionParameter))]
    [Union(1, typeof(DexihFunctionParameterBase))]
    public class DexihParameterBase : DexihHubNamedEntity
    {


        [Key(7)]
        public int Position { get; set; } = 0;

        [Key(8)]
        public EParameterDirection Direction { get; set; }

        [Key(9)]
        public ETypeCode DataType { get; set; }

        [Key(10)]
        public bool IsGeneric { get; set; }

        [Key(11)]
        public int Rank { get; set; } = 0;

        public bool IsInput() => Direction == EParameterDirection.Input || Direction == EParameterDirection.ResultInput || Direction == EParameterDirection.Join;

        public bool IsOutput() => Direction == EParameterDirection.Output ||
                                  Direction == EParameterDirection.ResultOutput ||
                                  Direction == EParameterDirection.ReturnValue ||
                                  Direction == EParameterDirection.ResultReturnValue;

    }
}
