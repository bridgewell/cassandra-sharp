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

namespace CassandraSharp.CQLBinaryProtocol
{
    using System;
    using System.Linq;
    using CassandraSharp.CQLBinaryProtocol.Queries;
    using CassandraSharp.Extensibility;

    internal sealed class PreparedQuery<T> : IPreparedQuery<T>
    {
        private readonly ICluster _cluster;

        private readonly ConsistencyLevel? _consistencyLevel;

        private readonly string _cql;

        private readonly ExecutionFlags? _executionFlags;

        private readonly IDataMapperFactory _factoryIn;

        private readonly IDataMapperFactory _factoryOut;

        private IColumnSpec[] _columnSpecs;

        /// <summary>
        /// connections which maintains by this prepared query.
        /// </summary>
        private volatile IConnection[] appendConnections;

        private volatile IConnection _connection;

        private byte[] _id;

        public PreparedQuery(ICluster cluster, ConsistencyLevel? consistencyLevel, ExecutionFlags? executionFlags, IDataMapperFactory factoryIn,
                             IDataMapperFactory factoryOut, string cql)
        {
            _cluster = cluster;
            _consistencyLevel = consistencyLevel;
            _executionFlags = executionFlags;
            _factoryIn = factoryIn;
            _factoryOut = factoryOut;
            _cql = cql;
            _connection = null;
            appendConnections = new IConnection[] { };
        }

        private IConnection peakConnection()
        {
            var currentConnections = appendConnections;
            var availableConnection = currentConnections.Where(c => c.GetAvailableStreamIdCount > Transport.LongRunningConnection.SUGGEST_AVIALABLE_STREAM_COUNT).FirstOrDefault();
            if (availableConnection != null)
                return availableConnection;

            lock (this)
            {
                // when we enter locked section, we can check again! 
                // in case we were blocked by previous lock, then we can get new one.
                currentConnections = appendConnections;
                availableConnection = currentConnections.Where(c => c.GetAvailableStreamIdCount > Transport.LongRunningConnection.SUGGEST_AVIALABLE_STREAM_COUNT).FirstOrDefault();
                if (availableConnection != null)
                    return availableConnection;

                // well, we were the first lock section, make a new connection!
                var newConnections = new IConnection[appendConnections.Length + 1];
                if (appendConnections.Length > 0)
                {
                    Array.Copy(appendConnections, 0, newConnections, 0, appendConnections.Length);
                    _cluster.GetLogger.Info("prepared query extended to connection count {0}", newConnections.Length);
                }

                var connection = _cluster.GetConnection(null, true);
                connection.OnFailure += ConnectionOnOnFailure;

                var cl = _consistencyLevel ?? connection.DefaultConsistencyLevel;
                var executionFlags = _executionFlags ?? connection.DefaultExecutionFlags;

                var futPrepare = new PrepareQuery(connection, cl, executionFlags, _cql).AsFuture();
                futPrepare.Wait();
                Tuple<byte[], IColumnSpec[]> preparedInfo = futPrepare.Result.Single();

                _id = preparedInfo.Item1;
                _columnSpecs = preparedInfo.Item2;
                newConnections[newConnections.Length - 1] = connection;

                appendConnections = newConnections;

                return connection;
            }
        }

        private IConnection prepareConnection()
        {
            IConnection connection;
            if (null == (connection = _connection))
            {
                lock (this)
                {
                    if (null == (connection = _connection))
                    {
                        connection = _cluster.GetConnection();
                        connection.OnFailure += ConnectionOnOnFailure;

                        var cl = _consistencyLevel ?? connection.DefaultConsistencyLevel;
                        var executionFlags = _executionFlags ?? connection.DefaultExecutionFlags;

                        var futPrepare = new PrepareQuery(connection, cl, executionFlags, _cql).AsFuture();
                        futPrepare.Wait();
                        Tuple<byte[], IColumnSpec[]> preparedInfo = futPrepare.Result.Single();

                        _id = preparedInfo.Item1;
                        _columnSpecs = preparedInfo.Item2;
                        _connection = connection;
                    }
                }
            }
            return _connection;
        }

        private IConnection getconn()
        {
            var conn = prepareConnection();
            if (conn.GetAvailableStreamIdCount > Transport.LongRunningConnection.SUGGEST_AVIALABLE_STREAM_COUNT)
            {
                return conn;
            }
            else
            {
                return peakConnection();
            }
        }

        public IQuery<T> Execute(object dataSource)
        {
            IConnection connection = getconn();
            var cl = _consistencyLevel ?? connection.DefaultConsistencyLevel;
            var executionFlags = _executionFlags ?? connection.DefaultExecutionFlags;
            IDataMapper mapperIn = _factoryIn.Create(dataSource.GetType());            
            IDataMapper mapperOut = _factoryOut.Create<T>();
            var futQuery = new ExecuteQuery<T>(connection, cl, executionFlags, _cql, _id, _columnSpecs, dataSource, mapperIn, mapperOut);
            return futQuery;
        }

        public void Dispose()
        {
            if (_connection != null)
            {
                _connection.OnFailure -= ConnectionOnOnFailure;
                _connection = null;
            }

            if (appendConnections.Length > 0)
            {
                lock (this)
                {
                    var connections = appendConnections;
                    foreach (var c in connections.Where(x => x != null))
                    {
                        c.OnFailure -= ConnectionOnOnFailure;
                        _cluster.DisposeConnection(c);
                    }                    
                    appendConnections = new IConnection[] { };
                }
            }
        }

        private void ConnectionOnOnFailure(object sender, FailureEventArgs failureEventArgs)
        {
            var connection = sender as IConnection;
            if (null != connection)
            {
                connection.OnFailure -= ConnectionOnOnFailure;
                if (_connection == connection)
                {
                    _connection = null;
                    return;
                }
                else
                {
                    // find the connection in connections and remove it.
                    lock (this)
                    {
                        var connections = appendConnections;
                        var leftConnections = connections.Where(c => c != connection).ToArray();
                        appendConnections = leftConnections;
                        _cluster.DisposeConnection(connection);
                    }
                }
            }
        }
    }
}