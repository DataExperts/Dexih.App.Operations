using System;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;

namespace dexih.operations.tests
{
    public class Serlialization
    {        
        
        private readonly ITestOutputHelper _output;

        // public Serlialization(ITestOutputHelper output)
        // {
        //     _output = output;
        //     
        //     var resolver = CompositeResolver.Create(
        //         new[] { MessagePack.Formatters.TypelessFormatter.Instance },
        //         new[] { MessagePack.Resolvers.StandardResolver.Instance });
        //     var options = MessagePackSerializerOptions.Standard.WithResolver(resolver);
        //     MessagePackSerializer.DefaultOptions = options;
        //
        // }
        
        public class IntKeySample
        {
            public int ABCDEF { get; set; } = 3;

            public int BCDEFG { get; set; } = 4;
        }

        // [Fact]
        // void Test_Serialize()
        // {
        //     var value = new [] { new IntKeySample(), new IntKeySample()};
        //     var options = MessagePackSerializerOptions.Standard.WithResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);
        //     var bytes = MessagePackSerializer.Serialize(value, options);
        //     _output.WriteLine($"Length: {bytes.Length}");
        //     _output.WriteLine(MessagePackSerializer.ConvertToJson(bytes));
        // }
        //
        // [Fact]
        // private void CacheManager_Serialize()
        // {
        //     var repositoryManager = MockHelpers.CreateRepositoryManager();
        //
        //     var cacheManager = new CacheManager(1, "abc");
        //     var returnValue = cacheManager.LoadGlobal("123", DateTime.Now);
        //     var bytes = MessagePackSerializer.Serialize(cacheManager);
        //     _output.WriteLine(MessagePackSerializer.ConvertToJson(bytes));
        //     var cacheManager2 = MessagePackSerializer.Deserialize<CacheManager>(bytes);
        //     
        //     Assert.Equal(cacheManager.HubKey, cacheManager2.HubKey);
        //     Assert.Equal(cacheManager.CacheEncryptionKey, cacheManager2.CacheEncryptionKey);
        //     Assert.Equal(cacheManager.DefaultRemoteLibraries.Connections.Count, cacheManager2.DefaultRemoteLibraries.Connections.Count);
        //     Assert.Equal(cacheManager.DefaultRemoteLibraries.Functions.Count, cacheManager2.DefaultRemoteLibraries.Functions.Count);
        //     Assert.Equal(cacheManager.DefaultRemoteLibraries.Transforms.Count, cacheManager2.DefaultRemoteLibraries.Transforms.Count);
        // }

        
        [Fact]
        public void RemoteSettings_Serlialize()
        {
            var json = "{\"appSettings\":{\"userPrompt\":false,\"remoteAgentId\":\"b68ea0cf-0f34-4286-8f8c-7d048ef34fc9\",\"user\":\"admin@dataexpertsgroup.com\",\"userToken\":\"CfDJ8DGgSO9GEh1LhZ/2CNKpiEPCNhG\u002BvD8jHklr9SgNFhmHFzkrJwaRN8mYScZWCx8aq/ghA6rKmpX/LxJx2mWCwhtsgFfKGXW/5mPasitXGCEihZuvh32l6OKByYjp5NlwNUZPqLK3xWLD9Ws/Wso6ZxQu0V8Gw5sWx1pSY8OqA3tTfYo7TKCkrd6rk\u002B4kVeuGIRBFpcS8ZUDa/TLfhpcHqCaDh4i/bbMut6suuRtwp8G0mMDlpZVR8HXnxXElO0Glkmy8NZc3rJ93PfmdSs6R50c=\",\"encryptionKey\":\"L5LC19lvhMxObSDoCpgNUjUSn3HBPTVDhy2/aAXUbpaZ8GH5Zr7QOF5IZe/3QbZ5gRI=\",\"webServer\":\"http://localhost:4200\",\"name\":\"Test Agent\",\"autoUpgrade\":false,\"allowPreReleases\":false,\"autoStartPath\":null},\"systemSettings\":{\"maxAcknowledgeWait\":5000,\"responseTimeout\":1000000,\"cancelDelay\":1000,\"encryptionIterations\":1000,\"maxPreviewDuration\":10000,\"maxConcurrentTasks\":50,\"maxUploadSize\":1000000000,\"socketTransportType\":\"WebSockets\"},\"logging\":{\"includeScopes\":false,\"logLevel\":{\"default\":0,\"system\":0,\"microsoft\":0},\"logFilePath\":\"/Users/garyholland/Downloads\"},\"runtime\":{\"configDirectory\":\"/Users/garyholland/Source/Dexih.App.Remote/src/dexih.remote\",\"appSettingsPath\":\"/Users/garyholland/Source/Dexih.App.Remote/src/dexih.remote/appsettings.json\",\"password\":null,\"localIpAddress\":\"192.168.1.14\",\"externalIpAddress\":null,\"defaultProxyUrl\":null,\"remoteAgentKey\":0,\"userHash\":null,\"version\":\"0.5.0-beta-00103\",\"latestVersion\":null,\"latestDownloadUrl\":null,\"generateUserToken\":false,\"saveSettings\":false,\"doUpgrade\":false},\"network\":{\"externalDownloadUrl\":null,\"localIpAddress\":null,\"localPort\":null,\"proxyUrl\":\"http://localhost:33440\",\"downloadPort\":33944,\"enforceHttps\":false,\"autoGenerateCertificate\":false,\"dynamicDomain\":\"dexih.com\",\"certificateFilename\":null,\"certificatePassword\":null,\"enableUPnP\":true},\"privacy\":{\"allowDataDownload\":true,\"allowDataUpload\":true,\"allowLanAccess\":true,\"allowExternalAccess\":false,\"allowProxy\":false},\"permissions\":{\"allowLocalFiles\":true,\"allowAllPaths\":true,\"allowedPaths\":[],\"allowAllHubs\":true,\"allowedHubs\":[]},\"namingStandards\":{}}";
//            var remoteSettings = new RemoteSettings();
//            var json = JsonSerializer.Serialize(remoteSettings, new JsonSerializerOptions() {IgnoreNullValues = true});
            var remoteSettings1 = JsonSerializer.Deserialize<object>(json, new JsonSerializerOptions() {IgnoreNullValues = true});

        }
    }
}