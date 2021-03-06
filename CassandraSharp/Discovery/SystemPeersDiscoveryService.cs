﻿// cassandra-sharp - high performance .NET driver for Apache Cassandra
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

namespace CassandraSharp.Discovery
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Numerics;
    using System.Timers;
    using CassandraSharp.CQLBinaryProtocol.Queries;
    using CassandraSharp.Config;
    using CassandraSharp.Enlightenment;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;

    internal sealed class SystemPeersDiscoveryService : IDiscoveryService
    {
        private readonly ICluster _cluster;

        private readonly ILogger _logger;

        private readonly IDataMapper _peerFactory;

        private readonly Timer _timer;

        private readonly DiscoveryConfig _config;

        public SystemPeersDiscoveryService(ILogger logger, ICluster cluster, DiscoveryConfig config)
        {
            IDataMapperFactory mapper = new PocoDataMapperFactory();
            _peerFactory = mapper.Create<DiscoveredPeer>();

            _logger = logger;
            _cluster = cluster;
            _config = config;
            if (config.Interval > 0)
            {
                _timer = new Timer(config.Interval * 1000);
                _timer.Elapsed += (s, e) => TryDiscover();
                _timer.AutoReset = true;
                _timer.Start();
            }

            TryDiscover();
        }

        public void Dispose()
        {
            _timer.SafeDispose();
        }

        public event TopologyUpdate OnTopologyUpdate;

        private void Notify(IPAddress rpcAddress, string datacenter, string rack, IEnumerable<string> tokens, string release_version, string current_version)
        {
            if (!Network.IsValidEndpoint(rpcAddress))
            {
                _logger.Warn("Discovered invalid endpoint {0}", rpcAddress);
                return;
            }
            
            if (release_version != current_version)
            {
                // skip mis matched version.
                return;
            }

            if (null != OnTopologyUpdate)
            {
                Peer peer = new Peer
                    {
                            RpcAddress = rpcAddress,
                            Datacenter = datacenter,
                            Rack = rack,
                            Tokens = tokens?.Select(BigInteger.Parse).ToArray()
                    };

                // see if we are at available datacenters.
                bool needDisCover = true;
                if (_config.DataCenter?.Length > 0)
                {
                    needDisCover = _config.DataCenter.Contains(datacenter);
                }
                _logger.Info("Discovered peer {0}, {1}, {2}, need:{3}", rpcAddress, datacenter, rack, needDisCover);

                if (needDisCover)
                    OnTopologyUpdate(NotificationKind.Update, peer);
                else
                    OnTopologyUpdate(NotificationKind.Remove, peer);
            }
        }

        private void TryDiscover()
        {
            try
            {
                // system.local contains release_version which current usage.
                // system.peers contains old versions, we only need current release version.

                IConnection connection = _cluster.GetConnection();

                var obsLocalPeer = new CqlQuery<DiscoveredPeer>(connection, ConsistencyLevel.ONE, ExecutionFlags.None, 
                    "select data_center,rack,tokens,release_version from system.local", _peerFactory);

                obsLocalPeer.Subscribe(x =>
                {
                    Notify(connection.Endpoint, x.Datacenter, x.Rack, x.Tokens, x.release_version, x.release_version);
                    var obsPeers = new CqlQuery<DiscoveredPeer>(connection, ConsistencyLevel.ONE, ExecutionFlags.None,
                        "select rpc_address,data_center,rack,tokens,release_version from system.peers", _peerFactory);
                    obsPeers.Subscribe(p => Notify(p.RpcAddress, p.Datacenter, p.Rack, p.Tokens, p.release_version, x.release_version),
                        ex => _logger.Error("SystemPeersDiscoveryService failed with error {0}", ex));
                }, ex => _logger.Error("SystemPeersDiscoveryService failed with error {0}", ex));

            }
            catch (Exception ex)
            {
                _logger.Error("SystemPeersDiscoveryService failed with error {0}", ex);
            }
        }

        internal class DiscoveredPeer
        {
            public IPAddress RpcAddress { get; internal set; }

            public HashSet<string> Tokens { get; internal set; }

            public string Rack { get; internal set; }

            public string Datacenter { get; internal set; }

            public string release_version { get; internal set; }
        }
    }
}