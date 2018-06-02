using System.ComponentModel.DataAnnotations.Schema;

namespace dexih.repository
{
    public class DexihActiveAgent
    {
        [NotMapped]
        public string RemoteAgentId { get; set; }
        
        [NotMapped]
        public string Name { get; set; }
        
        [NotMapped]
        public bool IsRunning { get; set; }
               
        [NotMapped]
        public string IpAddress { get; set; }
        
        [NotMapped]
        public string InstanceId { get; set; }

        [NotMapped]
        public bool IsEncrypted { get; set; }
        
        [NotMapped]
        public EDataPrivacyStatus DataPrivacyStatus { get; set; }

        [NotMapped]
        public DownloadUrl[] DownloadUrls { get; set; }


    }
}