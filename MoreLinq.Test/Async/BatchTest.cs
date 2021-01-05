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
    using System.Linq;
    using System.Runtime.CompilerServices;
    using System.Threading;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using MoreLinq.Experimental.Async;

    [TestFixture]
    public class BatchTest
    {
        [Test]
        public void BatchZeroSize()
        {
            AssertThrowsArgument.OutOfRangeException("size", () =>
                AsyncEnumerable.Empty<object>().Batch(0));
        }

        [Test]
        public void BatchNegativeSize()
        {
            AssertThrowsArgument.OutOfRangeException("size", () =>
                AsyncEnumerable.Empty<object>().Batch(-1));
        }

        [Test]
        public async Task BatchEvenlyDistributedAsyncSequence()
        {
            var result = new[] {1, 2, 3, 4, 5, 6, 7, 8, 9}.ToAsyncEnumerable().Batch(3);

            var e = result.GetAsyncEnumerator();
            await e.HasNextAsync(actual => new[] {1, 2, 3}.AssertSequenceEqual(actual));
            await e.HasNextAsync(actual => new[] {4, 5, 6}.AssertSequenceEqual(actual));
            await e.HasNextAsync(actual => new[] {7, 8, 9}.AssertSequenceEqual(actual));
            await e.NoNextAsync();
        }

        [Test]
        public async Task BatchUnevenlyDivisibleAsyncSequence()
        {
            var result = new[] {1, 2, 3, 4, 5, 6, 7, 8, 9}.ToAsyncEnumerable().Batch(4);

            var e = result.GetAsyncEnumerator();
            await e.HasNextAsync(actual => new[] {1, 2, 3, 4}.AssertSequenceEqual(actual));
            await e.HasNextAsync(actual => new[] {5, 6, 7, 8}.AssertSequenceEqual(actual));
            await e.HasNextAsync(actual => new[] {9}.AssertSequenceEqual(actual));
            await e.NoNextAsync();
        }

        [Test]
        public async Task BatchSequenceTransformingResult()
        {
            var result = new[] {1, 2, 3, 4, 5, 6, 7, 8, 9}.ToAsyncEnumerable()
                .Batch(4, batch => batch.Sum());

            var e = result.GetAsyncEnumerator();
            await e.HasNextAsync(10);
            await e.HasNextAsync(26);
            await e.HasNextAsync(9);
            await e.NoNextAsync();
        }

        [Test]
        public async Task BatchSequenceYieldsListsOfBatches()
        {
            var result = new[] {1, 2, 3}.ToAsyncEnumerable().Batch(2);

            var e = result.GetAsyncEnumerator();
            await e.HasNextAsync(actual => Assert.That(actual, Is.InstanceOf(typeof(IList<int>))));
            await e.HasNextAsync(actual => Assert.That(actual, Is.InstanceOf(typeof(IList<int>))));
            await e.NoNextAsync();
        }

        [Test]
        public void BatchIsLazy()
        {
            new BreakingAsyncSequence<object>().Batch(1);
        }

        [Test]
        public async Task BatchSequenceSmallerThanBinSize()
        {
            var result = new[] {1, 2, 3}.ToAsyncEnumerable().Batch(5);

            var e = result.GetAsyncEnumerator();
            await e.HasNextAsync(actual => actual.AssertSequenceEqual(new[] {1, 2, 3}));
            await e.NoNextAsync();
        }

        [TestCaseSource(nameof(TestAsyncEnumerable))]
        public async Task TestBatchPreservesElements(IAsyncEnumerable<int> enumerable,
            int expectedCount)
        {
            var result = enumerable.Batch(4);
            var actualCount = await result.SelectMany(en => en.ToAsyncEnumerable()).CountAsync();
            Assert.That(expectedCount, Is.EqualTo(actualCount));
        }

        [Test]
        public async Task BatchShouldCancel()
        {
            var cts = new CancellationTokenSource();
            var batchedSequence = Interval().Batch(1).WithCancellation(cts.Token);
            var e = batchedSequence.GetAsyncEnumerator();
            await e.HasNextAsync(actual => actual.AssertSequenceEqual(new[] {0}));
            cts.Cancel();
            Assert.That(async () => await e.MoveNextAsync(),
                Throws.InstanceOf<OperationCanceledException>());
        }

        private static async IAsyncEnumerable<int> Interval(int delay = 50,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            for (var i = 0;; ++i)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return i++;
                await Task.Delay(delay, cancellationToken);
            }
        }

        private static IEnumerable<TestCaseData> TestAsyncEnumerable() =>
            new[]
            {
                Array.Empty<int>(),
                new[] {1, 2, 3},
                new[] {1, 2, 3, 4},
                new[] {1, 2, 3, 4, 5}
            }.Select(en => new TestCaseData(en.ToAsyncEnumerable(), en.Length));
    }
}
