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

namespace CassandraSharp.Transport
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Net;
    using System.Net.Sockets;
    using System.Security.Authentication;
    using System.Threading;
    using System.Threading.Tasks;
    using CassandraSharp.CQLBinaryProtocol.Queries;
    using CassandraSharp.Config;
    using CassandraSharp.Exceptions;
    using CassandraSharp.Extensibility;
    using CassandraSharp.Utils;
    using System.Collections.Concurrent;

    internal sealed class LongRunningConnection : IConnection,
                                                  IDisposable
    {
        private const byte MAX_STREAMID = 0x80;

        private readonly TransportConfig _config;

        private readonly KeyspaceConfig _keyspaceConfig;

        private readonly IInstrumentation _instrumentation;

        private readonly ILogger _logger;

        /// <summary>
        /// available stream ids ,
        /// It should be easier to use BlockCollection class,
        /// but maybe BlockCollection would have GC issues for large use.
        /// And we could use eaiser byte array plus index to fix the problem.
        /// 1 means queries is using the index, we cannot use it as next query,
        /// 0 for no query using, so can use it.
        /// and use index to solve multi-thread issues.
        /// </summary>
        private byte[] _availableStreamIds = new byte[MAX_STREAMID];

        /// <summary>
        /// next available stream id index
        /// </summary>
        private int _availableStreamIdIndex = 0;

        /// <summary>
        /// pending queries before send to server
        /// </summary>
        private readonly BlockingCollection<QueryInfo> _pendingQueries = new BlockingCollection<QueryInfo>();

        private readonly CancellationTokenSource running;

        private readonly Action<QueryInfo, IFrameReader, bool> _pushResult;

        /// <summary>
        /// sent queries to server, but not get response yet.
        /// </summary>
        private readonly QueryInfo[] _queryInfos = new QueryInfo[MAX_STREAMID];

        private readonly Task _queryWorker;

        private readonly Task _responseWorker;

        private readonly Socket _socket;

        private readonly TcpClient _tcpClient;

        /// <summary>
        /// 1 means closed, 0 means running
        /// cannot use bool because Interlocked.Exchange does not support bool.
        /// </summary>
        private int _isClosed;

        public LongRunningConnection(IPAddress address, TransportConfig config, KeyspaceConfig keyspaceConfig, ILogger logger, IInstrumentation instrumentation)
        {
            try
            {
                // empty _availableStreamIds , indicates that we can use it.
                for (byte streamId = 0; streamId < MAX_STREAMID; ++streamId)
                {
                    _availableStreamIds[streamId] = 0;
                }
                running = new CancellationTokenSource();

                _config = config;
                _keyspaceConfig = keyspaceConfig;
                _logger = logger;
                _instrumentation = instrumentation;

                Endpoint = address;
                DefaultConsistencyLevel = config.DefaultConsistencyLevel;
                DefaultExecutionFlags = config.DefaultExecutionFlags;

                _tcpClient = new TcpClient
                    {
                        ReceiveTimeout = _config.ReceiveTimeout,
                        SendTimeout = _config.SendTimeout,
                        NoDelay = true,
                        LingerState = { Enabled = true, LingerTime = 0 },
                    };

                if(0 < _config.ConnectionTimeout)
                {
                    // TODO: refactor this and this probably is not robust in front of error
                    IAsyncResult asyncResult = _tcpClient.BeginConnect(address, _config.Port, null, null);
                    bool success = asyncResult.AsyncWaitHandle.WaitOne(TimeSpan.FromSeconds(_config.ConnectionTimeout), true);

                    if (! success)
                    {
                        throw new InvalidOperationException("Connection timeout occured.");
                    }

                    if (! _tcpClient.Connected)
                    {
                        _tcpClient.Close();
                        throw new InvalidOperationException("Can't connect to node.");
                    }

                    _tcpClient.EndConnect(asyncResult);
                }
                else
                {
                    _tcpClient.Connect(address, _config.Port);
                }
    
                _socket = _tcpClient.Client;

                _socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, _config.KeepAlive);
                if (_config.KeepAlive && 0 != _config.KeepAliveTime)
                {
                    SetTcpKeepAlive(_socket, _config.KeepAliveTime, 1000);
                }

                _pushResult = _config.ReceiveBuffering
                                      ? (Action<QueryInfo, IFrameReader, bool>)((qi, fr, a) => Task.Factory.StartNew(() => PushResult(qi, fr, a)))
                                      : PushResult;

                _responseWorker = Task.Factory.StartNew(() => RunWorker(ReadResponse), TaskCreationOptions.LongRunning);
                _queryWorker = Task.Factory.StartNew(() => RunWorker(SendQuery), TaskCreationOptions.LongRunning);

                // readify the connection
                _logger.Debug("Readyfying connection for {0}", Endpoint);
                //GetOptions();
                ReadifyConnection();
                _logger.Debug("Connection to {0} is ready", Endpoint);
            }
            catch (Exception ex)
            {
                Dispose();

                _logger.Error("Failed building connection {0}", ex);
                throw;
            }
        }

        public IPAddress Endpoint { get; private set; }

        public ConsistencyLevel DefaultConsistencyLevel { get; private set; }

        public ExecutionFlags DefaultExecutionFlags { get; private set; }

        public event EventHandler<FailureEventArgs> OnFailure;

        public void Execute<T>(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<T>> reader, InstrumentationToken token,
                               IObserver<T> observer)
        {
            if (_isClosed == 1)
            {
                var ex = new OperationCanceledException();
                OnFailure?.Invoke(this, new FailureEventArgs(ex));
                throw ex;
            }
            QueryInfo queryInfo = new QueryInfo<T>(writer, reader, token, observer);
            _pendingQueries.Add(queryInfo);
        }

        public void Dispose()
        {
            Close(null);

            // wait for worker threads to gracefully shutdown
            ExceptionExtensions.SafeExecute(() => _responseWorker.Wait());
            ExceptionExtensions.SafeExecute(() => _queryWorker.Wait());

            _pendingQueries.SafeDispose();
        }

        public static void SetTcpKeepAlive(Socket socket, int keepaliveTime, int keepaliveInterval)
        {
			if (Runtime.IsMono)
			{
				return;
			}

            // marshal the equivalent of the native structure into a byte array
            byte[] inOptionValues = new byte[12];

            int enable = 0 != keepaliveTime
                                 ? 1
                                 : 0;
            BitConverter.GetBytes(enable).CopyTo(inOptionValues, 0);
            BitConverter.GetBytes(keepaliveTime).CopyTo(inOptionValues, 4);
            BitConverter.GetBytes(keepaliveInterval).CopyTo(inOptionValues, 8);

            // write SIO_VALS to Socket IOControl
            socket.IOControl(IOControlCode.KeepAliveValues, inOptionValues, null);
        }

        private void Close(Exception ex)
        {
            // already in close state ?
            QueryInfo queryInfo;
            int wasClosed = Interlocked.Exchange(ref this._isClosed, 1);
            if (wasClosed == 1)
            {
                return;
            }
            running.Cancel();
            _pendingQueries.CompleteAdding();

            // abort all pending queries
            OperationCanceledException canceledException = new OperationCanceledException();
            for (int i = 0; i < _queryInfos.Length; ++i)
            {
                queryInfo = _queryInfos[i];
                if (null != queryInfo)
                {
                    queryInfo.NotifyError(canceledException);
                    _instrumentation.ClientTrace(queryInfo.Token, EventType.Cancellation);

                    _queryInfos[i] = null;
                }
            }
            // cleanup not sending queries.
            while (_pendingQueries.TryTake(out queryInfo))
            {
                queryInfo.NotifyError(canceledException);
                _instrumentation.ClientTrace(queryInfo.Token, EventType.Cancellation);
            }

            // we have now the guarantee this instance is destroyed once
            _tcpClient.SafeDispose();

            if (null != ex)
            {
                _logger.Fatal("Failed with error : {0}", ex);
                OnFailure?.Invoke(this, new FailureEventArgs(ex));
            }

            // release event to release observer
            OnFailure = null;
        }

        private void RunWorker(Action action)
        {
            try
            {
                action();
            }
            catch (Exception ex)
            {
                HandleError(ex);
            }
        }

        private byte AcquireNextStreamId()
        {
            var idx = Interlocked.Increment(ref _availableStreamIdIndex);
            if (idx < MAX_STREAMID)
            {
                var isUsing = _availableStreamIds[idx];
                if (isUsing == 0)
                {
                    _availableStreamIds[idx] = 1;
                    return (byte)idx;
                }
            }

            // now we need scan a loop, or reset the index to zero to find next available index
            // if we cannot find anyone, only to sleep a while, and find again.
            int SleepGapMS = 10;
            while (running.IsCancellationRequested == false)
            {
                // if we overflow, need reset
                if (idx >= MAX_STREAMID)
                {
                    var oldidx = Interlocked.CompareExchange(ref _availableStreamIdIndex, 0, idx);
                    if (oldidx != idx)
                    {
                        // exchanged failed, again
                        idx = Interlocked.Increment(ref _availableStreamIdIndex);
                        continue;
                    }
                }

                // find until overflow.
                while (idx < MAX_STREAMID)
                {
                    var isUsing = _availableStreamIds[idx];
                    if (isUsing == 0)
                    {
                        _availableStreamIds[idx] = 1;
                        return (byte)idx;
                    }
                }

                // failed , now we need sleep and try again.
                _logger.Warn("Cassandra driver LongRunning connection meet stream id exhausted, wait a while and try again.");
                Thread.Sleep(TimeSpan.FromMilliseconds(SleepGapMS));
                SleepGapMS = SleepGapMS * 2; // next time, sleep more!
            }

            // connection is closed!
            return 0;
        }    

        private void SendQuery()
        {
            foreach (var queryInfo in _pendingQueries.GetConsumingEnumerable())
            {
                byte streamId = AcquireNextStreamId();

                if (_isClosed == 1)
                {
                    // abort queries.
                    var ex = new OperationCanceledException();
                    queryInfo.NotifyError(ex);
                    _instrumentation.ClientTrace(queryInfo.Token, EventType.Cancellation);
                    throw ex;
                }

                try
                {
                    InstrumentationToken token = queryInfo.Token;
                    bool tracing = 0 != (token.ExecutionFlags & ExecutionFlags.ServerTracing);
                    using (var bufferingFrameWriter = new BufferingFrameWriter(tracing))
                    {
                        queryInfo.Write(bufferingFrameWriter);
                        
                        _logger.Debug("Starting writing frame for stream {0}@{1}", streamId, Endpoint);
                        _instrumentation.ClientTrace(token, EventType.BeginWrite);

                        _queryInfos[streamId] = queryInfo;
                        bufferingFrameWriter.SendFrame(streamId, _socket);

                        _logger.Debug("Done writing frame for stream {0}@{1}", streamId, Endpoint);
                        _instrumentation.ClientTrace(token, EventType.EndWrite);
                    }
                }
                catch (Exception ex)
                {
                    queryInfo.NotifyError(ex);

                    if (IsStreamInBadState(ex))
                    {
                        throw;
                    }
                }
            }
        }

        private void ReadResponse()
        {
            var token = running.Token;
            while (token.IsCancellationRequested == false)
            {
                IFrameReader frameReader = null;
                try
                {
                    frameReader = _config.ReceiveBuffering
                                          ? new BufferingFrameReader(_socket)
                                          : new StreamingFrameReader(_socket);

                    QueryInfo queryInfo = GetAndReleaseQueryInfo(frameReader);

                    _pushResult(queryInfo, frameReader, _config.ReceiveBuffering);
                }
                catch (Exception)
                {
                    frameReader.SafeDispose();
                    throw;
                }
            }
        }

        private QueryInfo GetAndReleaseQueryInfo(IFrameReader frameReader)
        {
            if (_isClosed == 1)
            {
                throw new OperationCanceledException();
            }

            QueryInfo queryInfo;
            byte streamId = frameReader.StreamId;
            queryInfo = _queryInfos[streamId];
            _queryInfos[streamId] = null;
            _availableStreamIds[streamId] = 0; // free the streamId

            return queryInfo;
        }

        private void PushResult(QueryInfo queryInfo, IFrameReader frameReader, bool isAsync)
        {
            try
            {
                _instrumentation.ClientTrace(queryInfo.Token, EventType.BeginRead);
                try
                {
                    if (null != frameReader.ResponseException)
                    {
                        throw frameReader.ResponseException;
                    }

                    queryInfo.Push(frameReader);
                }
                catch (Exception ex)
                {
                    queryInfo.NotifyError(ex);

                    if (IsStreamInBadState(ex))
                    {
                        throw;
                    }
                }

                _instrumentation.ClientTrace(queryInfo.Token, EventType.EndRead);

                InstrumentationToken token = queryInfo.Token;
                if (0 != (token.ExecutionFlags & ExecutionFlags.ServerTracing))
                {
                    _instrumentation.ServerTrace(token, frameReader.TraceId);
                }
            }
            catch (Exception ex)
            {
                if (isAsync)
                {
                    HandleError(ex);
                }
            }
            finally
            {
                if (isAsync)
                {
                    frameReader.SafeDispose();
                }
            }
        }

        private static bool IsStreamInBadState(Exception ex)
        {
            bool isFatal = ex is SocketException || ex is IOException || ex is TimeOutException;
            return isFatal;
        }

        private void HandleError(Exception ex)
        {
            Close(ex);
        }

        private void GetOptions()
        {
            var obsOptions = new CreateOptionsQuery(this, ConsistencyLevel.ONE, ExecutionFlags.None).AsFuture();
            obsOptions.Wait();
        }

        private void ReadifyConnection()
        {
            var obsReady = new ReadyQuery(this, ConsistencyLevel.ONE, ExecutionFlags.None, _config.CqlVersion).AsFuture();
            bool authenticate = obsReady.Result.Single();
            if (authenticate)
            {
                Authenticate();
            }

            if (!string.IsNullOrWhiteSpace(_keyspaceConfig.Name))
            {
                SetupKeyspace();
            }
        }

        private void Authenticate()
        {
            if (null == _config.User || null == _config.Password)
            {
                throw new InvalidCredentialException();
            }

            var obsAuth = new AuthenticateQuery(this, ConsistencyLevel.ONE, ExecutionFlags.None, _config.User, _config.Password).AsFuture();
            if (!obsAuth.Result.Single())
            {
                throw new InvalidCredentialException();
            }
        }

        private void SetupKeyspace()
        {
            var setKeyspaceQuery = new SetKeyspaceQuery(this, _keyspaceConfig.Name);

            try
            {
                setKeyspaceQuery.AsFuture().Wait();
            }
            catch
            {
                new CreateKeyspaceQuery(
                        this,
                        _keyspaceConfig.Name,
                        _keyspaceConfig.Replication.Options,
                        _keyspaceConfig.DurableWrites).AsFuture().Wait();
                setKeyspaceQuery.AsFuture().Wait();
            }

            _logger.Debug("Set default keyspace to {0}", _keyspaceConfig.Name);
        }

        private abstract class QueryInfo
        {
            protected QueryInfo(InstrumentationToken token)
            {
                Token = token;
            }

            public InstrumentationToken Token { get; private set; }

            public abstract void Write(IFrameWriter frameWriter);

            public abstract void Push(IFrameReader frameReader);

            public abstract void NotifyError(Exception ex);
        }

        private class QueryInfo<T> : QueryInfo
        {
            public QueryInfo(Action<IFrameWriter> writer, Func<IFrameReader, IEnumerable<T>> reader,
                             InstrumentationToken token, IObserver<T> observer)
                : base(token)
            {
                Writer = writer;
                Reader = reader;
                Observer = observer;
            }

            private Func<IFrameReader, IEnumerable<T>> Reader { get; set; }

            private Action<IFrameWriter> Writer { get; set; }

            private IObserver<T> Observer { get; set; }

            public override void Write(IFrameWriter frameWriter)
            {
                Writer(frameWriter);
            }

            public override void Push(IFrameReader frameReader)
            {
                IEnumerable<T> data = Reader(frameReader);
                foreach (T datum in data)
                {
                    Observer.OnNext(datum);
                }
                Observer.OnCompleted();
            }

            public override void NotifyError(Exception ex)
            {
                Observer.OnError(ex);
            }
        }
    }
}
