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
    using System.Threading;

    /// <summary>
    ///     Will loop through nodes to perfectly evenly spread load.
    /// </summary>
    internal sealed class RoundRobinEndpointStrategy : IEndpointStrategy
    {
        private IPAddress[] _bannedEndpoints;

        private IPAddress[] _healthyEndpoints;

        private int _nextCandidate;

        public RoundRobinEndpointStrategy(IEnumerable<IPAddress> endpoints)
        {
            _healthyEndpoints = endpoints.ToArray();
            _bannedEndpoints = new IPAddress[] { };
            _nextCandidate = 0;
        }

        public void Ban(IPAddress endpoint)
        {
            var newBan = new HashSet<IPAddress>(_bannedEndpoints);
            var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
            if (newHealthy.Remove(endpoint))
            {
                newBan.Add(endpoint);

                _bannedEndpoints = newBan.ToArray();
                _healthyEndpoints = newHealthy.ToArray();
            }
        }

        public void Permit(IPAddress endpoint)
        {
            var newBan = new HashSet<IPAddress>(_bannedEndpoints);
            var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
            if (newBan.Remove(endpoint))
            {
                newHealthy.Add(endpoint);

                _bannedEndpoints = newBan.ToArray();
                _healthyEndpoints = newHealthy.ToArray();
            }
        }

        public IPAddress Pick(BigInteger? token)
        {
            var healthy = _healthyEndpoints;
            switch(healthy.Length)
            {
                case 0:
                    return null;
                case 1:
                    return healthy[0];
            }

            var candidate = Interlocked.Increment(ref _nextCandidate);
            if (candidate < healthy.Length)
                return healthy[candidate];

            var fix_candidate = candidate % healthy.Length;
            Interlocked.CompareExchange(ref _nextCandidate, fix_candidate, candidate);
            return healthy[fix_candidate];
        }

        public void Update(NotificationKind kind, Peer peer)
        {
            IPAddress endpoint = peer.RpcAddress;
            switch (kind)
            {
                case NotificationKind.Add:
                case NotificationKind.Update:
                    {
                        var newBan = new HashSet<IPAddress>(_bannedEndpoints);
                        var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
                        if (!newHealthy.Contains(endpoint) && !newBan.Contains(endpoint))
                        {
                            newHealthy.Add(endpoint);
                            _healthyEndpoints = newHealthy.ToArray();
                        }
                    }
                    break;
                case NotificationKind.Remove:
                    {
                        var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
                        if (newHealthy.Remove(endpoint))
                        {
                            _healthyEndpoints = newHealthy.ToArray();
                        }
                    }
                    break;
            }
        }
    }
}