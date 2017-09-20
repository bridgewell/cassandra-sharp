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
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Numerics;
    using System.Linq;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal sealed class RandomEndpointStrategy : IEndpointStrategy
    {
        private IPAddress[] _bannedEndpoints;

        private IPAddress[] _healthyEndpoints;

        public RandomEndpointStrategy(IEnumerable<IPAddress> endpoints)
        {
            _healthyEndpoints = endpoints.ToArray();
            
            _bannedEndpoints = new IPAddress[] { };
        }

        public IPAddress Pick(BigInteger? token)
        {
            var currentHealthy = _healthyEndpoints;
            return (0 < currentHealthy.Length) ?
                currentHealthy[ThreadSafeRandom.Next(currentHealthy.Length)] :
                null;
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

        public void Update(NotificationKind kind, Peer peer)
        {
            var newBan = new HashSet<IPAddress>(_bannedEndpoints);
            var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
            IPAddress endpoint = peer.RpcAddress;
            switch (kind)
            {
                case NotificationKind.Remove:
                    if (newHealthy.Remove(endpoint))
                    {
                        _healthyEndpoints = newHealthy.ToArray();
                    }
                    if (newBan.Remove(endpoint))
                    {
                        _bannedEndpoints = newBan.ToArray();
                    }
                    break;
                case NotificationKind.Add:
                case NotificationKind.Update:
                    if (!newHealthy.Contains(endpoint) && !newBan.Contains(endpoint))
                    {
                        newHealthy.Add(endpoint);
                        _healthyEndpoints = newHealthy.ToArray();
                    }
                    break;
            }
        }
    }
}