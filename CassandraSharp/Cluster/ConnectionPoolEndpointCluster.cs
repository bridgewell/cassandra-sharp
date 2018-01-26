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

namespace CassandraSharp.Cluster
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Numerics;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;
    using System.Collections.Concurrent;
    using System.Linq;

    internal sealed class ConnectionPoolEndpointCluster : ICluster
    {
        private readonly IConnectionFactory _connectionFactory;

        private readonly IEndpointStrategy _endpointStrategy;

        private readonly ConcurrentDictionary<IPAddress, IConnection[]> _ip2Connection;

        private readonly ILogger _logger;

        private readonly IRecoveryService _recoveryService;

        public ConnectionPoolEndpointCluster(IEndpointStrategy endpointStrategy, ILogger logger,
                                                  IConnectionFactory connectionFactory, IRecoveryService recoveryService, IPartitioner partitioner)
        {
            _ip2Connection = new ConcurrentDictionary<IPAddress, IConnection[]>();
            _endpointStrategy = endpointStrategy;
            _logger = logger;
            _connectionFactory = connectionFactory;
            _recoveryService = recoveryService;
            Partitioner = partitioner;
            logger?.Info("ConnectionPoolEndpointCluster created.");
        }

        public ILogger GetLogger { get { return _logger; } }
        
        public IPartitioner Partitioner { get; private set; }

        public event ClusterClosed OnClosed;

        public void Dispose()
        {
            var allConnections = _ip2Connection.ToArray();
            _ip2Connection.Clear();

            foreach (var connection in allConnections.SelectMany(c => c.Value))
            {
                connection.SafeDispose();
            }

            if (null != OnClosed)
            {
                OnClosed();
                OnClosed = null;
            }
        }

        public IConnection GetConnection(BigInteger? token)
        {
            IConnection connection = null;
            try
            {
                while (null == connection)
                {
                    // pick and initialize a new endpoint connection
                    IPAddress endpoint = _endpointStrategy.Pick(token);
                    if (null == endpoint)
                    {
                        throw new ArgumentException("Can't find any valid endpoint");
                    }

                    var connections = _ip2Connection.GetOrAdd(endpoint, ep => new IConnection[] { });

                    var availableConnection = connections.Where(c => c.GetAvailableStreamIdCount > Transport.LongRunningConnection.SUGGEST_AVIALABLE_STREAM_COUNT).FirstOrDefault();
                    if (availableConnection != null)
                        return availableConnection;

                    // cannot find enough availableStreamId connections, try to create one.
                    connection = CreateTransportOrMarkEndpointForRecovery(endpoint);

                    if (connection != null)
                    {
                        _ip2Connection.AddOrUpdate(endpoint,
                            ep =>
                            {
                                return new IConnection[] { connection };
                            },
                            (ep, oldconnections) =>
                            {
                                var newConnections = new IConnection[oldconnections.Length + 1];
                                newConnections[0] = connection;
                                for (int i = 0; i < oldconnections.Length; i++)
                                    newConnections[i + 1] = oldconnections[i];
                                _logger.Info("endpoint {0} extend to connection count {1}", ep.ToString(), newConnections.Length);
                                return newConnections;
                            });
                    }
                }
                return connection;
            }
            catch
            {
                connection.SafeDispose();
                throw;
            }
        }

        private IConnection CreateTransportOrMarkEndpointForRecovery(IPAddress endpoint)
        {
            try
            {
                IConnection connection = _connectionFactory.Create(endpoint);
                connection.OnFailure += OnFailure;
                return connection;
            }
            catch (Exception ex)
            {
                _logger.Error("Error creating transport for endpoint {0} : {1}", endpoint, ex.Message);
                MarkEndpointForRecovery(endpoint);
            }

            return null;
        }

        private void OnFailure(object sender, FailureEventArgs e)
        {
            IConnection connection = sender as IConnection;
            if (null != connection && _ip2Connection.ContainsKey(connection.Endpoint))
            {
                IPAddress endpoint = connection.Endpoint;
                _logger.Error("connection {0} failed with error {1}", endpoint, e.Exception);
                IConnection[] connections;
                _ip2Connection.TryRemove(endpoint, out connections);
                bool isSenderDisposed = false;
                foreach (var con in connections)
                {
                    if (con == sender)
                    {
                        isSenderDisposed = true;
                    }
                    con.SafeDispose();
                }
                if (isSenderDisposed == false)
                {
                    sender.SafeDispose();
                }
                MarkEndpointForRecovery(endpoint);
            }
        }

        private void MarkEndpointForRecovery(IPAddress endpoint)
        {
            _logger.Info("marking {0} for recovery", endpoint);

            _endpointStrategy.Ban(endpoint);
            _recoveryService.Recover(endpoint, _connectionFactory, ClientRecoveredCallback);
        }

        private void ClientRecoveredCallback(IConnection connection)
        {
            _logger.Info("Endpoint {0} is recovered", connection.Endpoint);

            _endpointStrategy.Permit(connection.Endpoint);
            _ip2Connection.AddOrUpdate(connection.Endpoint,
                ep =>
                {
                    return new IConnection[] { connection };
                },
                (ep, oldconnections) =>
                {
                    var newConnections = new IConnection[oldconnections.Length + 1];
                    newConnections[0] = connection;
                    for (int i = 0; i < oldconnections.Length; i++)
                        newConnections[i + 1] = oldconnections[i];
                    return newConnections;
                });
            connection.OnFailure += OnFailure;
        }
    }
}