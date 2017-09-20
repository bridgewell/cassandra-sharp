using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraSharp.Utils
{
    public static class ThreadSafeRandom
    {
        private static Random _global = new Random();

        [ThreadStatic]
        private static Random _localrandom;

        public static Random localRandom
        {
            get
            {
                if (_localrandom == null)
                {
                    int seed;
                    lock (_global)
                        seed = _global.Next();
                    _localrandom = new Random(seed);
                }
                return _localrandom;
            }
        }

        public static double NextDouble()
        {
            return localRandom.NextDouble();
        }

        public static int Next(int maxValue)
        {
            return localRandom.Next(maxValue);
        }

        public static int Next()
        {
            return localRandom.Next();
        }
    }
}
