using Newtonsoft.Json;
using Newtonsoft.Json.Converters;

namespace dexih.repository
{
    public class EntityStatus
    {
        public EntityStatus()
        {
            LastStatus = EStatus.None;
            Message = "";
            IsBusy = false;
        }

        [JsonConverter(typeof(StringEnumConverter))]
        public enum EStatus
        {
            None, Ready, Imported, Updated, Added, Error
        }

        public EStatus LastStatus { get; set; }
        public string Message { get; set; }
        public bool IsBusy { get; set; }
    }
}
