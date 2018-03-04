using System.Collections.Generic;

namespace dexih.repository
{
    /// <summary>
    /// Transform Settings is used to pass various parameters when creating the transforms.
    /// </summary>
    public class TransformSettings
    {
        public IEnumerable<DexihHubVariable> HubVariables { get; set; }
        public RemoteSettings RemoteSettings { get; set; }
    }
}