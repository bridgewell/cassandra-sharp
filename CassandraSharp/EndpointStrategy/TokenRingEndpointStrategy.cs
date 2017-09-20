// cassandra-sharp - high performance .NET driver for Apache Cassandra
// Copyright (c) 2011-2013 Pierre Chalamet
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

namespace CassandraSharp.EndpointStrategy
{
    using System.Collections.Generic;
    using System.Net;
    using System.Numerics;
    using System.Linq;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;
    using System.Threading;

    /// <summary>
    ///     Will pick a node by it's token to choose the coordinator node by the row key of the query.
    ///     Requires QueryHints.
    ///     Where QueryHint's are ommitted, will fallback to RoundRobin
    /// </summary>
    internal sealed class TokenRingEndpointStrategy : IEndpointStrategy
    {
        private IPAddress[] _healthyEndpoints;

        private readonly TokenRing _ring;

        private int _nextCandidate;

        public TokenRingEndpointStrategy(IEnumerable<IPAddress> endpoints)
        {
            _healthyEndpoints = endpoints.ToArray();            
            _nextCandidate = 0;
            _ring = new TokenRing();
        }

        public void Ban(IPAddress endpoint)
        {
            var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
            if (newHealthy.Remove(endpoint))
            {
                lock(_ring)
                {
                    _ring.BanNode(endpoint);
                }
                _healthyEndpoints = newHealthy.ToArray();
            }
        }

        public void Permit(IPAddress endpoint)
        {
            var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
            newHealthy.Add(endpoint);
            _healthyEndpoints = newHealthy.ToArray();
            lock (_ring)
            {
                _ring.PermitNode(endpoint);
            }
        }

        public IPAddress Pick(BigInteger? token)
        {
            var currentHealthy = _healthyEndpoints;
            if (0 < currentHealthy.Length)
            {
                if (token.HasValue && 0 < _ring.RingSize())
                {
                    //Attempt to binary search for key in token ring
                    lock (_ring)
                    {
                        return _ring.FindReplica(token.Value);
                    }
                }
                else
                {
                    //fallback to round robin when no hint supplied
                    switch (currentHealthy.Length)
                    {
                        case 0:
                            return null;
                        case 1:
                            return currentHealthy[0];
                    }

                    int nextCandidate = Interlocked.Increment(ref _nextCandidate);
                    if (nextCandidate < currentHealthy.Length)
                        return currentHealthy[nextCandidate];

                    var fix_candidate = nextCandidate % currentHealthy.Length;
                    Interlocked.CompareExchange(ref _nextCandidate, fix_candidate, nextCandidate);
                    return currentHealthy[fix_candidate];                    
                }
            }

            return null;
        }

        public void Update(NotificationKind kind, Peer peer)
        {
            var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
            IPAddress endpoint = peer.RpcAddress;
            switch (kind)
            {
                case NotificationKind.Add:
                    if (!newHealthy.Contains(endpoint))
                    {
                        newHealthy.Add(endpoint);
                        _healthyEndpoints = newHealthy.ToArray();
                        lock (_ring)
                        {
                            _ring.AddOrUpdateNode(peer);
                        }
                    }
                    break;

                case NotificationKind.Update:
                    lock (_ring)
                    {
                        _ring.AddOrUpdateNode(peer);
                    }
                    break;

                case NotificationKind.Remove:
                    if (newHealthy.Contains(endpoint))
                    {
                        newHealthy.Remove(endpoint);
                        _healthyEndpoints = newHealthy.ToArray();
                        lock (_ring)
                        {
                            _ring.RemoveNode(endpoint);
                        }
                    }
                    break;
            }
        }
    }
}