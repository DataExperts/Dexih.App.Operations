using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using MessagePack;

namespace dexih.repository
{
    [MessagePackObject]
    public class EntityStatus
    {
        public EntityStatus()
        {
            LastStatus = EStatus.None;
            Message = "";
            IsBusy = false;
        }



        [Key(0)]
        public EStatus LastStatus { get; set; }

        [Key(1)]
        public string Message { get; set; }

        [Key(2)]
        public bool IsBusy { get; set; }
    }
}
