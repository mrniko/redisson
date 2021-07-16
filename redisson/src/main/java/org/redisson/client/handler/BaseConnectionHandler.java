/**
 * Copyright (c) 2013-2021 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.client.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.redisson.api.RFuture;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisClientConfig;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisLoadingException;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public abstract class BaseConnectionHandler<C extends RedisConnection> extends ChannelInboundHandlerAdapter {

    final RedisClient redisClient;
    final RPromise<C> connectionPromise = new RedissonPromise<C>();
    C connection;
    
    public BaseConnectionHandler(RedisClient redisClient) {
        super();
        this.redisClient = redisClient;
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        if (connection == null) {
            connection = createConnection(ctx);
        }
        super.channelRegistered(ctx);
    }

    abstract C createConnection(ChannelHandlerContext ctx);
    
    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        List<RFuture<Object>> futures = new ArrayList<RFuture<Object>>();

        RedisClientConfig config = redisClient.getConfig();
        if (config.getPassword() != null && !"".equals(config.getPassword())) {
            RFuture<Object> future;
            if (config.getUsername() != null) {
                future = connection.async(RedisCommands.AUTH, config.getUsername(), config.getPassword());
            } else {
                future = connection.async(RedisCommands.AUTH, config.getPassword());
            }
            futures.add(future);
        }
        if (config.getDatabase() != 0) {
            RFuture<Object> future = connection.async(RedisCommands.SELECT, config.getDatabase());
            futures.add(future);
        }
        if (config.getClientName() != null) {
            RFuture<Object> future = connection.async(RedisCommands.CLIENT_SETNAME, config.getClientName());
            futures.add(future);
        }
        if (config.isReadOnly()) {
            RFuture<Object> future = connection.async(RedisCommands.READONLY);
            futures.add(future);
        }
        if (config.getPingConnectionInterval() > 0) {
            RFuture<Object> future = connection.async(RedisCommands.PING);
            futures.add(future);
        }
        
        if (futures.isEmpty()) {
            ctx.fireChannelActive();
            connectionPromise.trySuccess(connection);
            return;
        }
        
        final AtomicBoolean retry = new AtomicBoolean();
        final AtomicInteger commandsCounter = new AtomicInteger(futures.size());
        for (RFuture<Object> future : futures) {
            future.onComplete((res, e) -> {
                if (e != null) {
                    if (e instanceof RedisLoadingException) {
                        if (retry.compareAndSet(false, true)) {
                            ctx.executor().schedule(() -> {
                                channelActive(ctx);
                            }, 1, TimeUnit.SECONDS);
                        }
                        return;
                    }
                    connection.closeAsync();
                    connectionPromise.tryFailure(e);
                    return;
                }
                if (commandsCounter.decrementAndGet() == 0) {
                    ctx.fireChannelActive();
                    connectionPromise.trySuccess(connection);
                }
            });
        }
    }
    
}
