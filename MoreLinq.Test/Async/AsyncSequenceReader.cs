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

namespace MoreLinq.Test.Async
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    internal static class AsyncSequenceReader
    {
        public static AsyncSequenceReader<T> Read<T>(this IAsyncEnumerable<T> source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            return new AsyncSequenceReader<T>(source);
        }
    }

    /// <summary>
    /// Adds reader semantics to a sequence where <see cref="IAsyncEnumerator{T}.MoveNextAsync"/>
    /// and <see cref="IAsyncEnumerator{T}.Current"/> are rolled into a single
    /// "read" operation.
    /// </summary>
    /// <typeparam name="T">Type of elements to read.</typeparam>
    internal class AsyncSequenceReader<T> : IAsyncDisposable
    {
        private IAsyncEnumerator<T> _enumerator;

        /// <summary>
        /// Initializes a <see cref="AsyncSequenceReader{T}" /> instance
        /// from an enumerable sequence.
        /// </summary>
        /// <param name="source">Source sequence.</param>
        public AsyncSequenceReader(IAsyncEnumerable<T> source) :
            this(GetEnumerator(source))
        {
        }

        /// <summary>
        /// Initializes a <see cref="SequenceReader{T}" /> instance
        /// from an enumerator.
        /// </summary>
        /// <param name="enumerator">Source enumerator.</param>
        public AsyncSequenceReader(IAsyncEnumerator<T> enumerator) =>
            _enumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));

        private static IAsyncEnumerator<T> GetEnumerator(IAsyncEnumerable<T> source)
        {
            if (source == null) throw new ArgumentNullException(nameof(source));
            return source.GetAsyncEnumerator();
        }

        /// <summary>
        /// Tries to read the next value.
        /// </summary>
        /// When this method returns, contains the value read on success.
        /// <returns>
        /// Returns true and the value if a value was successfully read; otherwise, false and the default value.
        /// </returns>
        public virtual async Task<(bool isPresent, T value)> TryReadAsync()
        {
            EnsureNotDisposed();

            var e = _enumerator;
            var hasNext = await e.MoveNextAsync();
            return hasNext
                ? (true, e.Current)
                : (false, default);
        }

        /// <summary>
        /// Reads a value otherwise throws <see cref="InvalidOperationException"/>
        /// if no more values are available.
        /// </summary>
        /// <returns>
        /// Returns the read value;
        /// </returns>
        public async Task<T> ReadAsync()
        {
            var (isPresent, value) = await TryReadAsync();
            return isPresent ? value : throw new InvalidOperationException();
        }

        /// <summary>
        /// Reads the end. If the end has not been reached then it
        /// throws <see cref="InvalidOperationException"/>.
        /// </summary>
        public virtual async Task ReadEndAsync()
        {
            EnsureNotDisposed();

            if (await _enumerator.MoveNextAsync())
                throw new InvalidOperationException();
        }

        /// <summary>
        /// Ensures that this object has not been disposed, that
        /// <see cref="Dispose"/> has not been previously called.
        /// </summary>
        protected void EnsureNotDisposed()
        {
            if (_enumerator == null)
                throw new ObjectDisposedException(GetType().FullName);
        }

        /// <summary>
        /// Disposes this object and enumerator with which is was
        /// initialized.
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            var e = _enumerator;
            if (e == null) return;
            _enumerator = null;
            await e.DisposeAsync();
        }
    }
}
