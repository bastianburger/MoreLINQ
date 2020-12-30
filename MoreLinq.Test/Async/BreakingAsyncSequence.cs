#region License and Terms

// MoreLINQ - Extensions to LINQ to Objects
// Copyright (c) 2008 Jonathan Skeet. All rights reserved.
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
    using System.Threading;

    /// <summary>
    /// Enumerable async sequence which throws InvalidOperationException as soon as its
    /// enumerator is requested. Used to check lazy evaluation.
    /// </summary>
    public class BreakingAsyncSequence<T> : IAsyncEnumerable<T>
    {
        IAsyncEnumerator<T> GetAsyncEnumerator(
            CancellationToken cancellationToken = default) =>
            throw new InvalidOperationException();

        IAsyncEnumerator<T> IAsyncEnumerable<T>.GetAsyncEnumerator(
            CancellationToken cancellationToken) => GetAsyncEnumerator(cancellationToken);
    }
}
