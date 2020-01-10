using System.Runtime.Serialization;


namespace dexih.repository
{
    [DataContract]
    public class EntityStatus
    {
        public EntityStatus()
        {
            LastStatus = EStatus.None;
            Message = "";
            IsBusy = false;
        }



        [DataMember(Order = 0)]
        public EStatus LastStatus { get; set; }

        [DataMember(Order = 1)]
        public string Message { get; set; }

        [DataMember(Order = 2)]
        public bool IsBusy { get; set; }
    }
}
