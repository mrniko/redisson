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
package org.redisson;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.core.RBucket;

import io.netty.util.concurrent.Future;

public class RedissonBucket<V> extends RedissonExpirable implements RBucket<V> {

  protected RedissonBucket(CommandAsyncExecutor connectionManager, String name) {
    super(connectionManager, name);
  }

  protected RedissonBucket(Codec codec, CommandAsyncExecutor connectionManager, String name) {
    super(codec, connectionManager, name);
  }

  @Override
  public boolean compareAndSet(V expect, V update) {
    return get(compareAndSetAsync(expect, update));
  }

  @Override
  public Future<Boolean> compareAndSetAsync(V expect, V update) {
    if (expect == null && update == null) {
      return trySetAsync(null);
    }

    if (expect == null) {
      return trySetAsync(update);
    }

    if (update == null) {
      return commandExecutor.evalWriteAsync(getName(), codec,
          RedisCommands.EVAL_BOOLEAN_WITH_VALUES, "if redis.call('get', KEYS[1]) == ARGV[1] then "
              + "redis.call('del', KEYS[1]); " + "return 1 " + "else " + "return 0 end",
          Collections.<Object>singletonList(getName()), expect);
    }

    return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_BOOLEAN_WITH_VALUES,
        "if redis.call('get', KEYS[1]) == ARGV[1] then " + "redis.call('set', KEYS[1], ARGV[2]); "
            + "return 1 " + "else " + "return 0 end", Collections.<Object>singletonList(getName()),
        expect, update);
  }

  @Override
  public V getAndSet(V newValue) {
    return get(getAndSetAsync(newValue));
  }

  @Override
  public Future<V> getAndSetAsync(V newValue) {
    if (newValue == null) {
      return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_OBJECT,
          "local v = redis.call('get', KEYS[1]); " + "redis.call('del', KEYS[1]); " + "return v",
          Collections.<Object>singletonList(getName()));
    }

    return commandExecutor.writeAsync(getName(), codec, RedisCommands.GETSET, getName(), newValue);
  }

  @Override
  public V get() {
    return get(getAsync());
  }

  @Override
  public Future<V> getAsync() {
    return commandExecutor.readAsync(getName(), codec, RedisCommands.GET, getName());
  }

  @Override
  public void set(V value) {
    get(setAsync(value));
  }

  @Override
  public Future<Void> setAsync(V value) {
    if (value == null) {
      return commandExecutor.writeAsync(getName(), RedisCommands.DEL_VOID, getName());
    }

    return commandExecutor.writeAsync(getName(), codec, RedisCommands.SET, getName(), value);
  }

  @Override
  public void set(V value, long timeToLive, TimeUnit timeUnit) {
    get(setAsync(value, timeToLive, timeUnit));
  }

  @Override
  public Future<Void> setAsync(V value, long timeToLive, TimeUnit timeUnit) {
    if (value == null) {
      throw new IllegalArgumentException("Value can't be null");
    }

    return commandExecutor.writeAsync(getName(), codec, RedisCommands.SETEX, getName(),
        timeUnit.toSeconds(timeToLive), value);
  }

  /**
   * Use {@link #isExistsAsync()}
   *
   * @return
   */
  @Deprecated
  public Future<Boolean> existsAsync() {
    return isExistsAsync();
  }

  /**
   * Use {@link #isExists()}
   *
   * @return
   */
  @Deprecated
  public boolean exists() {
    return isExists();
  }

  @Override
  public Future<Boolean> trySetAsync(V value) {
    if (value == null) {
      return commandExecutor.readAsync(getName(), codec, RedisCommands.NOT_EXISTS, getName());
    }

    return commandExecutor.writeAsync(getName(), codec, RedisCommands.SETNX, getName(), value);
  }

  @Override
  public Future<Boolean> trySetAsync(V value, long timeToLive, TimeUnit timeUnit) {
    if (value == null) {
      throw new IllegalArgumentException("Value can't be null");
    }
    return commandExecutor.writeAsync(getName(), codec, RedisCommands.SETPXNX, getName(), value,
        "PX", timeUnit.toMillis(timeToLive), "NX");
  }

  @Override
  public boolean trySet(V value, long timeToLive, TimeUnit timeUnit) {
    return get(trySetAsync(value, timeToLive, timeUnit));
  }

  @Override
  public boolean trySet(V value) {
    return get(trySetAsync(value));
  }

}
