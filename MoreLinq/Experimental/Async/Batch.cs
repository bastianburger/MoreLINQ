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
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;

    static partial class MoreAsyncEnumerable
    {
        /// <summary>
        /// Batches the source sequence into sized buckets and depends on the AsyncEnumerable implementation of Batch.
        /// </summary>
        /// <typeparam name="TSource">The source sequence.</typeparam>
        /// <param name="source">The source sequence.</param>
        /// <param name="size">Size of buckets.</param>
        /// <returns>A sequence of equally sized buckets containing elements of the source collection.</returns>
        public static IEnumerable<IEnumerable<TSource>> Batch<TSource>(
            this IEnumerable<TSource> source, int size) =>
            Batch(source.ToMoreLinqEnumerable(), size, x => x).ToEnumerable();

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

            return BatchInternal(source.ToMoreLinqEnumerable(), size, resultSelector);
        }

        private static async IAsyncEnumerable<TResult> BatchInternal<TSource, TResult>(
            this MoreLinqEnumerable<TSource> source, int size,
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

        internal static MoreLinqEnumerable<T> ToMoreLinqEnumerable<T>(this IEnumerable<T> source) =>
            new MoreLinqEnumerable<T>(source);

        internal static MoreLinqEnumerable<T> ToMoreLinqEnumerable<T>(this IAsyncEnumerable<T> source) =>
            new MoreLinqEnumerable<T>(source);

        internal static IEnumerable<T> ToEnumerable<T>(this MoreLinqEnumerable<T> source) =>
            source.ToEnumerable();

        internal static IAsyncEnumerable<T> ToAsyncEnumerable<T>(this MoreLinqEnumerable<T> source) =>
            source.AsyncSource!;
    }

    /// <summary>
    /// Represents an async sequence that MoreLinq uses internally for computations
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal struct MoreLinqEnumerable<T> : IAsyncEnumerable<T>
    {
        public readonly IEnumerable<T>? SyncSource { get; }
        public readonly IAsyncEnumerable<T>? AsyncSource { get; }

        public MoreLinqEnumerable(IEnumerable<T> source) => (SyncSource, AsyncSource) = (source, null);
        public MoreLinqEnumerable(IAsyncEnumerable<T> source) => (SyncSource, AsyncSource) = (null, source);

        IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(CancellationToken cancellationToken)
        {
            if (SyncSource is { })
            {
                return new MoreLinqSyncAsyncEnumerator<T>(SyncSource.GetEnumerator());
            }
            else
            {
                return AsyncSource!.GetAsyncEnumerator();
            }
        }
    }

    /// <summary>
    /// Wrapper around a synchronous enumerator exposed via the IAsyncEnumerator interface.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal struct MoreLinqSyncAsyncEnumerator<T> : IAsyncDisposable, IAsyncEnumerator<T>
    {
        private readonly IEnumerator<T> _enumerator;

        public MoreLinqSyncAsyncEnumerator(IEnumerator<T> enumerator) =>
            _enumerator = enumerator;

        public ValueTask<bool> MoveNextAsync() =>
            new ValueTask<bool>(_enumerator.MoveNext());

        public ValueTask DisposeAsync() => new ValueTask();

        public T Current { get => _enumerator.Current; }
    }
}

#endif // !NO_ASYNC_STREAMS
