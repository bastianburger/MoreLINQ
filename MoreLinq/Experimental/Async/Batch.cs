#region License and Terms

// MoreLINQ - Extensions to LINQ to Objects
// Copyright (c) 2020 Atif Aziz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#endregion

#if !NO_ASYNC_STREAMS

namespace MoreLinq.Experimental.Async
{
    using System;
    using System.Collections.Generic;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    static partial class MoreAsyncEnumerable
    {
        /// <summary>
        /// Batches the source sequence into sized buckets.
        /// </summary>
        /// <param name="source">The source sequence.</param>
        /// <param name="size">Size of buckets.</param>
        /// <typeparam name="TSource">Type of elements in <paramref name="source"/> sequence.</typeparam>
        /// <returns>An asynchronous sequence of equally sized buckets containing elements of the source collection.</returns>
        public static IAsyncEnumerable<IEnumerable<TSource>> Batch<TSource>(
            this IAsyncEnumerable<TSource> source, int size) =>
            Batch(source, size, x => x);

        /// <summary>
        /// Batches the source sequence into sized buckets and applies a projection to each bucket.
        /// </summary>
        /// <param name="source">The source sequence.</param>
        /// <param name="size">Size of buckets.</param>
        /// <param name="resultSelector"></param>
        /// <typeparam name="TSource">Type of elements in <paramref name="source"/> sequence.</typeparam>
        /// <typeparam name="TResult">Type of result returned by <paramref name="resultSelector"/>.</typeparam>
        /// <returns>An asynchronous sequence of equally sized buckets containing elements of the source collection.</returns>
        public static IAsyncEnumerable<TResult> Batch<TSource, TResult>(
            this IAsyncEnumerable<TSource> source, int size,
            Func<IEnumerable<TSource>, TResult> resultSelector)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            if (size <= 0) throw new ArgumentOutOfRangeException(nameof(size));
            if (resultSelector == null) throw new ArgumentNullException(nameof(resultSelector));

            return BatchInternal(source, size, resultSelector);
        }

        private static async IAsyncEnumerable<TResult> BatchInternal<TSource, TResult>(
            this IAsyncEnumerable<TSource> source, int size,
            Func<IEnumerable<TSource>, TResult> resultSelector,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            TSource[]? bucket = null;
            var count = 0;

            await foreach (var item in source.WithCancellation(cancellationToken)
                .ConfigureAwait(false))
            {
                // what do we need to do with cancellation, if the source does not support it? should we invoke
                // cancellationToken.ThrowIfCancellationRequested(); here to still cancel the operation?
                bucket ??= new TSource[size];
                bucket[count++] = item;

                // The bucket is fully buffered before it's yielded
                if (count < size)
                    continue;

                yield return resultSelector(bucket);

                bucket = null;
                count = 0;
            }

            // Return the last bucket with all remaining elements
            if (bucket != null)
            {
                Array.Resize(ref bucket, count);
                yield return resultSelector(bucket);
            }
        }
    }
}

#endif // !NO_ASYNC_STREAMS
