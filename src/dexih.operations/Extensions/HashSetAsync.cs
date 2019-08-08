using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore.Extensions.Internal;

namespace dexih.operations.Extensions
{
    public static class HashSetAsync
    {
        public static async Task<HashSet<TSource>> ToHashSetAsync<TSource>(
            this IQueryable<TSource> source,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var enumerator = source.AsAsyncEnumerable().GetEnumerator();
            
            var hashSet = new HashSet<TSource>();

            while (await enumerator.MoveNext())
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    throw new OperationCanceledException();
                }
                
                hashSet.Add(enumerator.Current);
            }

            return hashSet;
        }
    }
}