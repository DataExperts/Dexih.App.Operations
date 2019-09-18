using System;
using System.Diagnostics;
using MessagePack;
using Xunit;
using Xunit.Abstractions;

namespace dexih.operations.tests
{
    public class Serlialization
    {        
        
        private readonly ITestOutputHelper _output;

        public Serlialization(ITestOutputHelper output)
        {
            _output = output;
        }
        
        public class IntKeySample
        {
            public int ABCDEF { get; set; } = 3;

            public int BCDEFG { get; set; } = 4;
        }

        [Fact]
        void Test_Serlialize()
        {
            var value = new [] { new IntKeySample(), new IntKeySample()};
            var bytes = MessagePackSerializer.Serialize(value, MessagePack.Resolvers.ContractlessStandardResolver.Instance);
            _output.WriteLine($"Length: {bytes.Length}");
            _output.WriteLine(MessagePackSerializer.ToJson(bytes));
        }
        
        [Fact]
        private void CacheManager_Serlialize()
        {
            var repositoryManager = MockHelpers.CreateRepositoryManager();

            var cacheManager = new CacheManager(1, "abc");
            var returnValue = cacheManager.LoadGlobal("123", DateTime.Now, repositoryManager.DbContext);
            MessagePackSerializer.SetDefaultResolver(MessagePack.Resolvers.ContractlessStandardResolver.Instance);
            var bytes = MessagePackSerializer.Serialize(cacheManager);
            _output.WriteLine(MessagePackSerializer.ToJson(bytes));
            var cacheManager2 = MessagePackSerializer.Deserialize<CacheManager>(bytes);
            
            Assert.Equal(cacheManager.HubKey, cacheManager2.HubKey);
            Assert.Equal(cacheManager.CacheEncryptionKey, cacheManager2.CacheEncryptionKey);
            Assert.Equal(cacheManager.DefaultRemoteLibraries.Connections.Count, cacheManager2.DefaultRemoteLibraries.Connections.Count);
            Assert.Equal(cacheManager.DefaultRemoteLibraries.Functions.Count, cacheManager2.DefaultRemoteLibraries.Functions.Count);
            Assert.Equal(cacheManager.DefaultRemoteLibraries.Transforms.Count, cacheManager2.DefaultRemoteLibraries.Transforms.Count);

        }
    }
}