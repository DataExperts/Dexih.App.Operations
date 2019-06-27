using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Caching.Memory;
using Newtonsoft.Json;

namespace dexih.operations.Extensions
{
    public static class CachingExtensions
    {
        
        /// <summary>
        /// Two layered cache.  First checks memory cache, then checks distributed cache for the required value.
        /// </summary>
        /// <param name="cache"></param>
        /// <param name="memoryCache"></param>
        /// <param name="key"></param>
        /// <param name="factory"></param>
        /// <param name="cancellationToken"></param>
        /// <typeparam name="TItem"></typeparam>
        /// <returns></returns>
        public static Task<TItem> GetOrCreateAsync<TItem>(
            this IDistributedCache cache,
            IMemoryCache memoryCache,
            string key,
            Func<DistributedCacheEntryOptions, Task<TItem>> factory, CancellationToken cancellationToken = default)
        {
            return memoryCache.GetOrCreateAsync<TItem>(key, async entry =>
            {
                var value = await cache.GetAsync(key, cancellationToken);
                if (value == null)
                {
                    var options = new DistributedCacheEntryOptions();
                    var obj = await factory.Invoke(options);

                    entry.SlidingExpiration = options.SlidingExpiration;

                    var json = JsonConvert.SerializeObject(obj);
                    var binary = Encoding.ASCII.GetBytes(json);
                    
                    try
                    {
                        await cache.SetAsync(key, binary, options, cancellationToken);
                        return obj;
                    }
                    catch (Exception)
                    {
                        value = await cache.GetAsync(key, cancellationToken);

                        if (value == null)
                        {
                            throw;
                        }
                    }

                }

                var json2 = Encoding.ASCII.GetString(value);
                return JsonConvert.DeserializeObject<TItem>(json2);
                
            });
        }
        
    }
}