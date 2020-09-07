using System.Runtime.Serialization;
using Dexih.Utils.CopyProperties;



namespace dexih.repository
{
    [DataContract]
    // [Union(0, typeof(DexihRemoteAgentHub))]
    // [Union(1, typeof(DexihHubNamedEntity))]
    public class DexihHubKeyEntity : DexihHubEntity
    {
        [DataMember(Order = 4)]
        [CopyCollectionKey((long)0, true)]
        public long Key { get; set; }
        
        public virtual void ResetKeys()
        {
            Key = 0;
        }
        
    }
    
  
}
