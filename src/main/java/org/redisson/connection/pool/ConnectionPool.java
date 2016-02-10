/**
 * Copyright 2014 Nikita Koksharov, Nickolay Borbit
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.redisson.connection.pool;

import java.net.InetSocketAddress;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.redisson.MasterSlaveServersConfig;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.connection.ClientConnectionsEntry;
import org.redisson.connection.ClientConnectionsEntry.FreezeReason;
import org.redisson.connection.ClientConnectionsEntry.NodeType;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.MasterSlaveEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

abstract class ConnectionPool<T extends RedisConnection> {

  private final Logger log = LoggerFactory.getLogger(getClass());

  protected final List<ClientConnectionsEntry> entries =
      new CopyOnWriteArrayList<ClientConnectionsEntry>();

  final ConnectionManager connectionManager;

  final MasterSlaveServersConfig config;

  final MasterSlaveEntry masterSlaveEntry;

  public ConnectionPool(MasterSlaveServersConfig config, ConnectionManager connectionManager,
      MasterSlaveEntry masterSlaveEntry) {
    this.config = config;
    this.masterSlaveEntry = masterSlaveEntry;
    this.connectionManager = connectionManager;
  }

  public Future<Void> add(final ClientConnectionsEntry entry) {
    final Promise<Void> promise = connectionManager.newPromise();
    promise.addListener(new FutureListener<Void>() {
      @Override
      public void operationComplete(Future<Void> future) throws Exception {
        entries.add(entry);
      }
    });
    initConnections(entry, promise, true);
    return promise;
  }

  private void initConnections(final ClientConnectionsEntry entry, final Promise<Void> initPromise,
      boolean checkFreezed) {
    final int minimumIdleSize = getMinimumIdleSize(entry);

    if (minimumIdleSize == 0 || (checkFreezed && entry.isFreezed())) {
      initPromise.setSuccess(null);
      return;
    }

    final AtomicInteger initializedConnections = new AtomicInteger(minimumIdleSize);
    int startAmount = Math.min(50, minimumIdleSize);
    final AtomicInteger requests = new AtomicInteger(startAmount);
    for (int i = 0; i < startAmount; i++) {
      createConnection(checkFreezed, requests, entry, initPromise, minimumIdleSize,
          initializedConnections);
    }
  }

  private void createConnection(final boolean checkFreezed, final AtomicInteger requests,
      final ClientConnectionsEntry entry, final Promise<Void> initPromise,
      final int minimumIdleSize, final AtomicInteger initializedConnections) {

    if ((checkFreezed && entry.isFreezed()) || !tryAcquireConnection(entry)) {
      Throwable cause =
          new RedisConnectionException("Can't init enough connections amount! Only "
              + (minimumIdleSize - initializedConnections.get()) + " from " + minimumIdleSize
              + " were initialized. Server: " + entry.getClient().getAddr());
      initPromise.tryFailure(cause);
      return;
    }

    Future<T> promise = createConnection(entry);
    promise.addListener(new FutureListener<T>() {
      @Override
      public void operationComplete(Future<T> future) throws Exception {
        if (future.isSuccess()) {
          T conn = future.getNow();
          releaseConnection(entry, conn);
        }

        releaseConnection(entry);

        if (!future.isSuccess()) {
          Throwable cause =
              new RedisConnectionException("Can't init enough connections amount! Only "
                  + (minimumIdleSize - initializedConnections.get()) + " from " + minimumIdleSize
                  + " were initialized. Server: " + entry.getClient().getAddr(), future.cause());
          initPromise.tryFailure(cause);
          return;
        }

        int value = initializedConnections.decrementAndGet();
        if (value == 0) {
          log.info("{} connections initialized for {}", minimumIdleSize, entry.getClient()
              .getAddr());
          initPromise.setSuccess(null);
        } else if (value > 0 && !initPromise.isDone()) {
          if (requests.incrementAndGet() <= minimumIdleSize) {
            createConnection(checkFreezed, requests, entry, initPromise, minimumIdleSize,
                initializedConnections);
          }
        }
      }
    });
  }

  protected abstract int getMinimumIdleSize(ClientConnectionsEntry entry);

  protected ClientConnectionsEntry getEntry() {
    return config.getLoadBalancer().getEntry(entries);
  }

  public Future<T> get() {
    for (int j = entries.size() - 1; j >= 0; j--) {
      ClientConnectionsEntry entry = getEntry();
      if (!entry.isFreezed() && tryAcquireConnection(entry)) {
        return connectTo(entry);
      }
    }

    List<InetSocketAddress> zeroConnectionsAmount = new LinkedList<InetSocketAddress>();
    List<InetSocketAddress> freezed = new LinkedList<InetSocketAddress>();
    for (ClientConnectionsEntry entry : entries) {
      if (entry.isFreezed()) {
        freezed.add(entry.getClient().getAddr());
      } else {
        zeroConnectionsAmount.add(entry.getClient().getAddr());
      }
    }

    StringBuilder errorMsg =
        new StringBuilder(
            "Connection pool exhausted! All connections are busy. Try to increase connection pool size.");
    // if (!freezed.isEmpty()) {
    // errorMsg.append(" Disconnected hosts: " + freezed);
    // }
    if (!zeroConnectionsAmount.isEmpty()) {
      errorMsg.append(" Hosts with fully busy connections: " + zeroConnectionsAmount);
    }

    RedisConnectionException exception = new RedisConnectionException(errorMsg.toString());
    return connectionManager.newFailedFuture(exception);
  }

  public Future<T> get(ClientConnectionsEntry entry) {
    if (((entry.getNodeType() == NodeType.MASTER && entry.getFreezeReason() == FreezeReason.SYSTEM) || !entry
        .isFreezed()) && tryAcquireConnection(entry)) {
      return connectTo(entry);
    }

    RedisConnectionException exception =
        new RedisConnectionException("Can't aquire connection to " + entry.getClient().getAddr());
    return connectionManager.newFailedFuture(exception);
  }

  protected boolean tryAcquireConnection(ClientConnectionsEntry entry) {
    return entry.getFailedAttempts() < config.getFailedAttempts() && entry.tryAcquireConnection();
  }

  protected T poll(ClientConnectionsEntry entry) {
    return (T) entry.pollConnection();
  }

  protected Future<T> connect(ClientConnectionsEntry entry) {
    return (Future<T>) entry.connect(config);
  }

  private Future<T> connectTo(final ClientConnectionsEntry entry) {
    T conn = poll(entry);
    if (conn != null) {
      if (!conn.isActive()) {
        return promiseFailure(entry, conn);
      }

      return promiseSuccessful(entry, conn);
    }

    return createConnection(entry);
  }

  private Future<T> createConnection(final ClientConnectionsEntry entry) {
    final Promise<T> promise = connectionManager.newPromise();
    Future<T> connFuture = connect(entry);
    connFuture.addListener(new FutureListener<T>() {
      @Override
      public void operationComplete(Future<T> future) throws Exception {
        if (!future.isSuccess()) {
          releaseConnection(entry);

          promiseFailure(entry, promise, future.cause());
          return;
        }

        T conn = future.getNow();
        if (!conn.isActive()) {
          promiseFailure(entry, promise, conn);
          return;
        }

        promiseSuccessful(entry, promise, conn);
      }
    });
    return promise;
  }

  private void promiseSuccessful(ClientConnectionsEntry entry, Promise<T> promise, T conn) {
    entry.resetFailedAttempts();
    if (!promise.trySuccess(conn)) {
      releaseConnection(entry, conn);
      releaseConnection(entry);
    }
  }

  private Future<T> promiseSuccessful(ClientConnectionsEntry entry, T conn) {
    entry.resetFailedAttempts();
    return (Future<T>) conn.getAcquireFuture();
  }

  private void promiseFailure(ClientConnectionsEntry entry, Promise<T> promise, Throwable cause) {
    if (entry.incFailedAttempts() == config.getFailedAttempts()) {
      checkForReconnect(entry);
    }

    promise.tryFailure(cause);
  }

  private void promiseFailure(ClientConnectionsEntry entry, Promise<T> promise, T conn) {
    int attempts = entry.incFailedAttempts();
    if (attempts == config.getFailedAttempts()) {
      checkForReconnect(entry);
    } else if (attempts < config.getFailedAttempts()) {
      releaseConnection(entry, conn);
    }

    releaseConnection(entry);

    RedisConnectionException cause = new RedisConnectionException(conn + " is not active!");
    promise.tryFailure(cause);
  }

  private Future<T> promiseFailure(ClientConnectionsEntry entry, T conn) {
    int attempts = entry.incFailedAttempts();
    if (attempts == config.getFailedAttempts()) {
      checkForReconnect(entry);
    } else if (attempts < config.getFailedAttempts()) {
      releaseConnection(entry, conn);
    }

    releaseConnection(entry);

    RedisConnectionException cause = new RedisConnectionException(conn + " is not active!");
    return connectionManager.newFailedFuture(cause);
  }

  private void checkForReconnect(ClientConnectionsEntry entry) {
    if (entry.getNodeType() == NodeType.SLAVE) {
      connectionManager.slaveDown(masterSlaveEntry, entry.getClient().getAddr().getHostName(),
          entry.getClient().getAddr().getPort(), FreezeReason.RECONNECT);
      log.warn("slave {} disconnected due to failedAttempts={} limit reached", entry.getClient()
          .getAddr(), config.getFailedAttempts());
      scheduleCheck(entry);
    } else {
      if (entry.freezeMaster(FreezeReason.RECONNECT)) {
        log.warn("host {} disconnected due to failedAttempts={} limit reached", entry.getClient()
            .getAddr(), config.getFailedAttempts());
        scheduleCheck(entry);
      }
    }
  }

  private void scheduleCheck(final ClientConnectionsEntry entry) {

    connectionManager.getConnectionEventsHub().fireDisconnect(entry.getClient().getAddr());

    connectionManager.newTimeout(new TimerTask() {
      @Override
      public void run(Timeout timeout) throws Exception {
        if (entry.getFreezeReason() != FreezeReason.RECONNECT || !entry.isFreezed()) {
          return;
        }

        Future<RedisConnection> connectionFuture = entry.getClient().connectAsync();
        connectionFuture.addListener(new FutureListener<RedisConnection>() {
          @Override
          public void operationComplete(Future<RedisConnection> future) throws Exception {
            if (entry.getFreezeReason() != FreezeReason.RECONNECT || !entry.isFreezed()) {
              return;
            }

            if (!future.isSuccess()) {
              scheduleCheck(entry);
              return;
            }
            final RedisConnection c = future.getNow();
            if (!c.isActive()) {
              c.closeAsync();
              scheduleCheck(entry);
              return;
            }

            Future<String> f = c.asyncWithTimeout(null, RedisCommands.PING);
            f.addListener(new FutureListener<String>() {
              @Override
              public void operationComplete(Future<String> future) throws Exception {
                try {
                  if (entry.getFreezeReason() != FreezeReason.RECONNECT || !entry.isFreezed()) {
                    return;
                  }

                  if (future.isSuccess() && "PONG".equals(future.getNow())) {
                    entry.resetFailedAttempts();
                    Promise<Void> promise = connectionManager.newPromise();
                    promise.addListener(new FutureListener<Void>() {
                      @Override
                      public void operationComplete(Future<Void> future) throws Exception {
                        if (entry.getNodeType() == NodeType.SLAVE) {
                          masterSlaveEntry.slaveUp(entry.getClient().getAddr().getHostName(), entry
                              .getClient().getAddr().getPort(), FreezeReason.RECONNECT);
                          log.info("slave {} successfully reconnected", entry.getClient().getAddr());
                        } else {
                          synchronized (entry) {
                            if (entry.getFreezeReason() == FreezeReason.RECONNECT) {
                              entry.setFreezed(false);
                              entry.setFreezeReason(null);
                              log.info("host {} successfully reconnected", entry.getClient()
                                  .getAddr());
                            }
                          }
                        }
                      }
                    });
                    initConnections(entry, promise, false);
                  } else {
                    scheduleCheck(entry);
                  }
                } finally {
                  c.closeAsync();
                }
              }
            });
          }
        });
      }
    }, config.getReconnectionTimeout(), TimeUnit.MILLISECONDS);
  }

  public void returnConnection(ClientConnectionsEntry entry, T connection) {
    if (entry.isFreezed()) {
      connection.closeAsync();
    } else {
      releaseConnection(entry, connection);
    }
    releaseConnection(entry);
  }

  protected void releaseConnection(ClientConnectionsEntry entry) {
    entry.releaseConnection();
  }

  protected void releaseConnection(ClientConnectionsEntry entry, T conn) {
    entry.releaseConnection(conn);
  }

}
