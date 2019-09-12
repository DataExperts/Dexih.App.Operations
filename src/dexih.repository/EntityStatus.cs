using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using ProtoBuf;

namespace dexih.repository
{
    [ProtoContract]
    public class EntityStatus
    {
        public EntityStatus()
        {
            LastStatus = EStatus.None;
            Message = "";
            IsBusy = false;
        }



        [ProtoMember(1)]
        public EStatus LastStatus { get; set; }

        [ProtoMember(2)]
        public string Message { get; set; }

        [ProtoMember(3)]
        public bool IsBusy { get; set; }
    }
}
