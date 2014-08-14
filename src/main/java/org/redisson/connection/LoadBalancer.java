/**
 * Copyright 2014 Nikita Koksharov, Nickolay Borbit
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
package org.redisson.connection;

import java.util.Collection;

import org.redisson.MasterSlaveServersConfig;

import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;

public interface LoadBalancer {

    void shutdown();

    void unfreeze(String host, int port);

    Collection<RedisPubSubConnection> freeze(String host, int port);

    void init(RedisCodec codec, MasterSlaveServersConfig config);

    void add(SubscribesConnectionEntry entry);

    RedisConnection nextConnection();

    RedisPubSubConnection nextPubSubConnection();

    void returnConnection(RedisConnection connection);

    void returnSubscribeConnection(RedisPubSubConnection connection);

}
