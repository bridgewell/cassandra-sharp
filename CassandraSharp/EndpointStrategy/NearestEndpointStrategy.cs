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
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;
    using System.Linq;

    internal sealed class NearestEndpointStrategy : IEndpointStrategy
    {
        private IPAddress[] _bannedEndpoints;

        private IPAddress[] _healthyEndpoints;

        private readonly IPAddress _clientAddress;

        private readonly IEndpointSnitch _snitch;

        private readonly EndpointComparer epComparer;

        public NearestEndpointStrategy(IEnumerable<IPAddress> endpoints, IEndpointSnitch snitch)
        {
            _snitch = snitch;
            _clientAddress = Network.Find(Dns.GetHostName());
            if (null == _clientAddress)
            {
                throw new ArgumentException("Failed to resolve IP for client address");
            }

            _healthyEndpoints = snitch.GetSortedListByProximity(_clientAddress, endpoints).ToArray();
            _bannedEndpoints = new IPAddress[] { };
            epComparer = new EndpointComparer(_snitch, _clientAddress);
        }

        public IPAddress Pick(BigInteger? token)
        {
            var currentHealthyPoints = _healthyEndpoints;
            return (0 < currentHealthyPoints.Length) ? currentHealthyPoints[0] : null;
        }

        public void Ban(IPAddress endPoint)
        {
            var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
            var newBan = new HashSet<IPAddress>(_bannedEndpoints);
            if (newHealthy.Remove(endPoint))
            {
                newBan.Add(endPoint);
                _healthyEndpoints = newHealthy.ToArray();
                _bannedEndpoints = newBan.ToArray();
            }
        }

        public void Permit(IPAddress endPoint)
        {
            var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
            var newBan = new HashSet<IPAddress>(_bannedEndpoints);
            if (newBan.Remove(endPoint))
            {
                newHealthy.Add(endPoint);
                _healthyEndpoints = newHealthy.OrderBy(e => e, epComparer).ToArray();
                _bannedEndpoints = newBan.ToArray();
            }
        }

        public void Update(NotificationKind kind, Peer peer)
        {
            var newHealthy = new HashSet<IPAddress>(_healthyEndpoints);
            var newBan = new HashSet<IPAddress>(_bannedEndpoints);
            IPAddress endpoint = peer.RpcAddress;
            switch (kind)
            {
                case NotificationKind.Remove:
                    if (newHealthy.Remove(endpoint))
                    {
                        _healthyEndpoints = newHealthy.OrderBy(e => e, epComparer).ToArray();
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
                        _healthyEndpoints = newHealthy.OrderBy(e => e, epComparer).ToArray();
                    }
                    break;
            }
        }
    }

    class EndpointComparer : IComparer<IPAddress>
    {
        private IPAddress _client;
        private IEndpointSnitch _snitch;

        public EndpointComparer(IEndpointSnitch snitch, IPAddress clientAddress)
        {
            _snitch = snitch;
            _client = clientAddress;
        }
        public int Compare(IPAddress x, IPAddress y)
        {
            return _snitch.CompareEndpoints(_client, x, y);
        }
    }
}