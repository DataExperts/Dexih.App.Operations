using System.ComponentModel.DataAnnotations.Schema;
using System.Runtime.Serialization;
using System.Text.Json.Serialization;
using Dexih.Utils.CopyProperties;

namespace dexih.repository
{
    [DataContract]
    public class DexihIssueComment: DexihBaseEntity
    {
        [DataMember(Order = 4)]
        [CopyParentCollectionKey]
        public long IssueKey { get; set; }
        
        [DataMember(Order = 5)]
        [CopyCollectionKey((long)0, true)]
        public long Key { get; set; }

        [DataMember(Order = 6)]
        public string Comment { get; set; }

        [DataMember(Order = 7)]
        public string UserId { get; set; }

        [JsonIgnore, CopyIgnore, IgnoreDataMember, NotMapped]
        public DexihIssue Issue { get; set; }
        
        [DataMember(Order = 19), NotMapped]
        public string UserName { get; set; }

    }
}