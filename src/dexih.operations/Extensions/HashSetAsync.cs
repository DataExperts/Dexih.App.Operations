using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace dexih.operations.Extensions
{
    public static class HashSetAsync
    {
        public static async Task<HashSet<TSource>> ToHashSetAsync<TSource>(
            this IQueryable<TSource> source,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var hashSet = new HashSet<TSource>();

            await foreach (var item in source.AsAsyncEnumerable().WithCancellation(cancellationToken))
            {
                hashSet.Add(item);
            }
            
            return hashSet;
        }
    }
}