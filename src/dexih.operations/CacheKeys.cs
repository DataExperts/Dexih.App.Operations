namespace dexih.operations
{
    public class CacheKeys
    {
        public static string UserHubs(string userId) => $"USERHUBS-{userId}";
        public static string AdminHubs => "ADMINHUBS";
        public static string HubUserIds(long hubKey) => $"HUBUSERIDS-{hubKey}";
        public static string HubUsers(long hubKey) => $"HUBUSERS-{hubKey}";
        public static string Hub(long hubKey) => $"HUB-{hubKey}";
        public static string UserHubPermission(string userId, long hubKey) => $"PERMISSION-USER-{userId}-HUB-{hubKey}";
        public static string RemoteAgentHubs(string remoteAgentId) => $"REMOTEAGENT-HUBS-{remoteAgentId}";
        public static string RemoteAgentUserHubs(string userId) => $"REMOTEAGENT-USER-HUBS-{userId}";
    }
}