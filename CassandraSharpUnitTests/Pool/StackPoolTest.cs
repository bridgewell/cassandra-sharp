﻿// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
// limitations under the License.
namespace CassandraSharpUnitTests.Pool
{
    using System;
    using CassandraSharp.Pool;
    using Moq;
    using NUnit.Framework;

    [TestFixture]
    public class StackPoolTest
    {
        [Test]
        public void TestBehavior()
        {
            Mock<IDisposable> mock1 = new Mock<IDisposable>();
            Mock<IDisposable> mock2 = new Mock<IDisposable>();
            Mock<IDisposable> mock3 = new Mock<IDisposable>();

            IPool<IDisposable> pool = new StackPool<IDisposable>(2);

            // nothing should be aquired
            IDisposable disposable;
            Assert.IsFalse(pool.Acquire(out disposable));
            Assert.IsNull(disposable);

            // no dispose (1 bucket free)
            pool.Release(mock1.Object);
            mock1.Verify(x => x.Dispose(), Times.Never());

            // acquire should be ok
            Assert.IsTrue(pool.Acquire(out disposable));
            mock1.Verify(x => x.Dispose(), Times.Never());
            Assert.IsNotNull(disposable);

            // no release (0 bucket free)
            pool.Release(disposable);
            pool.Release(mock2.Object);
            mock1.Verify(x => x.Dispose(), Times.Never());
            mock2.Verify(x => x.Dispose(), Times.Never());

            // should release since no free bucket
            pool.Release(mock3.Object);
            mock3.Verify(x => x.Dispose());

            // dispose must dispose everything
            pool.Dispose();
            mock1.Verify(x => x.Dispose());
            mock2.Verify(x => x.Dispose());
        }
    }
}