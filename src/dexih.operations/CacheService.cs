using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;


namespace dexih.operations
{
    public interface ICacheService
    {
        IMemoryCache MemoryCache { get; }
        IDistributedCache DistributedCache { get; }
        
        Task<TItem> GetOrCreateAsync<TItem>(
            string key,
            TimeSpan expiration,
            Func<Task<TItem>> factory,
            CancellationToken cancellationToken = default);

        Task<T> Get<T>(string key, CancellationToken cancellationToken);

        Task Update<T>(string key, CancellationToken cancellationToken = default);
        
        Task Reset(string key, CancellationToken cancellationToken = default);
        Task ResetDistributed(string key, CancellationToken cancellationToken = default);
    }

    public class CacheServiceItem<T>
    {
        public long UpdatedDate { get; set; }
        public T Item { get; set; }

        public CacheServiceItem(T item)
        {
            Item = item;
            UpdatedDate = DateTime.Now.Ticks;
        }
    }
    
    public class CacheService: ICacheService
    {
        public CacheService(IDistributedCache distributedCache)
        {
            MemoryCache = new MemoryCache(new MemoryCacheOptions());
            DistributedCache = distributedCache;
        }

        public IMemoryCache MemoryCache { get; }
        public IDistributedCache DistributedCache { get; }

        
        /// <summary>
        /// Two layered cache.  First checks memory cache, then checks distributed cache for the required value.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="expiration"></param>
        /// <param name="factory"></param>
        /// <param name="cancellationToken"></param>
        /// <typeparam name="TItem"></typeparam>
        /// <returns></returns>
        public async Task<TItem> GetOrCreateAsync<TItem>(
            string key,
            TimeSpan expiration,
            Func<Task<TItem>> factory, 
            CancellationToken cancellationToken = default)
        {

            var updated = 0L;

            // check distributed cache for an update
            if (DistributedCache != null)
            {
                var bytes = await DistributedCache.GetAsync(key, cancellationToken);
                if (bytes != null)
                {
                    updated = BitConverter.ToInt64(bytes);
                }
            }
            
            var cacheItem = await MemoryCache.GetOrCreateAsync(key, async entry =>
            {
                entry.SetSlidingExpiration(expiration);
                var value = await factory.Invoke();
                return new CacheServiceItem<TItem>(value);
            });

            // if the distributed version was updated more recently than the current, the refresh the cache.
            if (updated > cacheItem.UpdatedDate)
            {
                var value = await factory.Invoke();
                var options = new MemoryCacheEntryOptions() {SlidingExpiration = expiration};
                MemoryCache.Set(key, new CacheServiceItem<TItem>(value), options);
            }
            
            return cacheItem.Item;
        }

        public async Task<T> Get<T>(string key, CancellationToken cancellationToken)
        {
            if (MemoryCache.TryGetValue<CacheServiceItem<T>>(key, out var item))
            {
                var updated = 0L;
                
                // check distributed cache for an update
                if (DistributedCache != null)
                {
                    var bytes = await DistributedCache.GetAsync(key, cancellationToken);
                    if (bytes != null)
                    {
                        updated = BitConverter.ToInt64(bytes);
                    }
                }
                
                // if the distributed version was updated more recently than the current, return null, and clear cache
                if (updated > item.UpdatedDate)
                {
                    MemoryCache.Remove(key);
                    return default;
                }

                return item.Item;
            }

            return default;
        }

        public Task Reset(string key, CancellationToken cancellationToken = default)
        {
            MemoryCache.Remove(key);
            return ResetDistributed(key, cancellationToken);
        }

        public async Task Update<T>(string key, CancellationToken cancellationToken = default)
        {
            await ResetDistributed(key, cancellationToken);
            if (MemoryCache.TryGetValue<CacheServiceItem<T>>(key, out var item))
            {
                item.UpdatedDate = DateTime.Now.Ticks;
            }
        }

        public Task ResetDistributed(string key, CancellationToken cancellationToken = default)
        {
            if (DistributedCache != null)
            {
                var bytes = BitConverter.GetBytes(DateTime.Now.Ticks);
                var distributedOptions = new DistributedCacheEntryOptions();
                return DistributedCache.SetAsync(key, bytes, distributedOptions, cancellationToken);
            }
            return Task.CompletedTask;
        }
    }
}