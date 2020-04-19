using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [DataContract]
    public class DexihIssue : DexihHubNamedEntity
    {

        public DexihIssue()
        {
            DexihIssueComments = new HashSet<DexihIssueComment>();
        }
        
        [DataMember(Order = 7)]
        public EIssueType Type { get; set; }
        
        [DataMember(Order = 8)]
        public EIssueCategory Category { get; set; }
        
        [DataMember(Order = 9)]
        public EIssueSeverity Severity { get; set; }
        
        [DataMember(Order = 10)]
        public string Link { get; set; }
        
        [DataMember(Order = 11)]
        public string Data { get; set; }
        
        [DataMember(Order = 12)]
        public string GitHubLink { get; set; }

        [DataMember(Order = 13)] 
        public bool IsPrivate { get; set; } = true;

        [DataMember(Order = 14)]
        public string UserId { get; set; }
        
        [DataMember(Order = 15)]
        public EIssueStatus IssueStatus { get; set; }
        
        [DataMember(Order = 16)]
        public ICollection<DexihIssueComment> DexihIssueComments { get; set; }
        
        [DataMember(Order = 17), NotMapped]
        public string UserName { get; set; }
        
    }
}