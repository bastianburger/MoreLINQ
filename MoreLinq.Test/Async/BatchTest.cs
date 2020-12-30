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
    using System.Linq;
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

            await using var reader = result.Read();
            (await reader.ReadAsync()).AssertSequenceEqual(new[] {1, 2, 3});
            (await reader.ReadAsync()).AssertSequenceEqual(new[] {4, 5, 6});
            (await reader.ReadAsync()).AssertSequenceEqual(new[] {7, 8, 9});
            await reader.ReadEndAsync();
        }

        [Test]
        public async Task BatchUnevenlyDivisibleAsyncSequence()
        {
            var result = new[] {1, 2, 3, 4, 5, 6, 7, 8, 9}.ToAsyncEnumerable().Batch(4);

            await using var reader = result.Read();
            (await reader.ReadAsync()).AssertSequenceEqual(new[] {1, 2, 3, 4});
            (await reader.ReadAsync()).AssertSequenceEqual(new[] {5, 6, 7, 8});
            (await reader.ReadAsync()).AssertSequenceEqual(new[] {9});
            await reader.ReadEndAsync();
        }

        [Test]
        public async Task BatchSequenceTransformingResult()
        {
            var result = new[] {1, 2, 3, 4, 5, 6, 7, 8, 9}.ToAsyncEnumerable()
                .Batch(4, batch => batch.Sum());

            await using var reader = result.Read();
            Assert.That(await reader.ReadAsync(), Is.EqualTo(10));
            Assert.That(await reader.ReadAsync(), Is.EqualTo(26));
            Assert.That(await reader.ReadAsync(), Is.EqualTo(9));
            await reader.ReadEndAsync();
        }
    }
}
