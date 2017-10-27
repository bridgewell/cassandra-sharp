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

namespace CassandraSharp.Recovery
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using CassandraSharp.Config;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;
    using System.Threading.Tasks;
    using System.Collections.Concurrent;
    using System.Threading;

    internal sealed class AttemptConnectRecoveryService : IRecoveryService
    {
        private readonly ILogger _logger;

        private readonly Task _recoveryTask;

        private readonly List<RecoveryItem> _toRecover;

        /// <summary>
        /// use to read/write _toRecover
        /// </summary>
        private readonly object _lock;

        private readonly CancellationTokenSource taskCTS;

        public AttemptConnectRecoveryService(ILogger logger, RecoveryConfig config)
        {
            _lock = new object();
            _logger = logger;
            _toRecover = new List<RecoveryItem>();
            taskCTS = new CancellationTokenSource();
            _recoveryTask = Task.Factory.StartNew(
                async () =>
                {
                    var token = taskCTS.Token;
                    while (token.IsCancellationRequested == false)
                    {
                        TryRecover();
                        await Task.Delay(TimeSpan.FromSeconds(10), token);
                    }
                });
        }

        public void Recover(IPAddress endpoint, IConnectionFactory connectionFactory, Action<IConnection> clientRecoveredCallback)
        {
            lock (_lock)
            {
                _toRecover.Add(new RecoveryItem(endpoint, connectionFactory, clientRecoveredCallback));
            }
        }

        public void Dispose()
        {
            taskCTS.Cancel();
        }

        private void TryRecover()
        {
            RecoveryItem[] toRecover;
            lock (_lock)
            {
                toRecover = _toRecover.ToArray();
            }

            foreach (RecoveryItem recoveryItem in toRecover)
            {
                try
                {
                    _logger.Debug("Trying to count recover endpoint {0}", recoveryItem.Endpoint);
                    recoveryItem.RecoveryTryCount++;
                    if (recoveryItem.RecoveryBackwardCounter <= 0)
                    {
                        _logger.Debug("Trying to recover endpoint {0}", recoveryItem.Endpoint);
                        IConnection client = recoveryItem.ConnectionFactory.Create(recoveryItem.Endpoint);
                        _logger.Debug("Endpoint {0} successfully recovered", recoveryItem.Endpoint);

                        lock (_lock)
                        {
                            _toRecover.Remove(recoveryItem);
                        }

                        recoveryItem.ClientRecoveredCallback(client);
                    }
                    else
                    {
                        recoveryItem.RecoveryBackwardCounter--;
                    }
                }
                catch (Exception ex)
                {
                    recoveryItem.RecoveryBackwardCounter = 1 << recoveryItem.RecoveryTryCount;
                    _logger.Debug("Failed to recover endpoint {0} with error {1}", recoveryItem.Endpoint, ex);
                }
            }
        }

        private class RecoveryItem
        {
            public RecoveryItem(IPAddress endpoint, IConnectionFactory connectionFactory, Action<IConnection> clientRecoveredCallback)
            {
                Endpoint = endpoint;
                ConnectionFactory = connectionFactory;
                ClientRecoveredCallback = clientRecoveredCallback;
                RecoveryTryCount = 0;
                RecoveryBackwardCounter = 0;
            }

            public IPAddress Endpoint { get; private set; }

            public IConnectionFactory ConnectionFactory { get; private set; }

            public Action<IConnection> ClientRecoveredCallback { get; private set; }

            public int RecoveryTryCount { get; set; }

            public int RecoveryBackwardCounter { get; set; }
        }
    }
}