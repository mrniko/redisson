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
package org.redisson.reactive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

import org.reactivestreams.Publisher;
import org.redisson.api.RBitSetReactive;
import org.redisson.client.codec.BitSetCodec;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandBatchService;
import org.redisson.command.CommandReactiveExecutor;

import reactor.rx.Streams;

public class RedissonBitSetReactive extends RedissonExpirableReactive implements RBitSetReactive {

  public RedissonBitSetReactive(CommandReactiveExecutor connectionManager, String name) {
    super(connectionManager, name);
  }

  public Publisher<Boolean> get(long bitIndex) {
    return commandExecutor
        .readReactive(getName(), codec, RedisCommands.GETBIT, getName(), bitIndex);
  }

  public Publisher<Void> set(long bitIndex, boolean value) {
    return commandExecutor.writeReactive(getName(), codec, RedisCommands.SETBIT_VOID, getName(),
        bitIndex, value ? 1 : 0);
  }

  public Publisher<byte[]> toByteArray() {
    return commandExecutor.readReactive(getName(), ByteArrayCodec.INSTANCE, RedisCommands.GET,
        getName());
  }

  private Publisher<Void> op(String op, String... bitSetNames) {
    List<Object> params = new ArrayList<Object>(bitSetNames.length + 3);
    params.add(op);
    params.add(getName());
    params.add(getName());
    params.addAll(Arrays.asList(bitSetNames));
    return commandExecutor.writeReactive(getName(), codec, RedisCommands.BITOP, params.toArray());
  }

  public Publisher<BitSet> asBitSet() {
    return commandExecutor.readReactive(getName(), BitSetCodec.INSTANCE, RedisCommands.GET,
        getName());
  }

  // Copied from: https://github.com/xetorthio/jedis/issues/301
  private static byte[] toByteArrayReverse(BitSet bits) {
    byte[] bytes = new byte[bits.length() / 8 + 1];
    for (int i = 0; i < bits.length(); i++) {
      if (bits.get(i)) {
        final int value = bytes[i / 8] | (1 << (7 - (i % 8)));
        bytes[i / 8] = (byte) value;
      }
    }
    return bytes;
  }

  @Override
  public Publisher<Long> length() {
    return commandExecutor.evalReadReactive(getName(), codec, RedisCommands.EVAL_LONG,
        "local fromBit = redis.call('bitpos', KEYS[1], 1, -1);"
            + "local toBit = 8*(fromBit/8 + 1) - fromBit % 8;" + "for i = toBit, fromBit, -1 do "
            + "if redis.call('getbit', KEYS[1], i) == 1 then " + "return i+1;" + "end;" + "end;"
            + "return fromBit+1", Collections.<Object>singletonList(getName()));
  }

  @Override
  public Publisher<Void> set(long fromIndex, long toIndex, boolean value) {
    if (value) {
      return set(fromIndex, toIndex);
    }
    return clear(fromIndex, toIndex);
  }

  @Override
  public Publisher<Void> clear(long fromIndex, long toIndex) {
    CommandBatchService executorService =
        new CommandBatchService(commandExecutor.getConnectionManager());
    for (long i = fromIndex; i < toIndex; i++) {
      executorService.writeAsync(getName(), codec, RedisCommands.SETBIT_VOID, getName(), i, 0);
    }
    return new NettyFuturePublisher<Void>(executorService.executeAsyncVoid());
  }

  @Override
  public Publisher<Void> set(BitSet bs) {
    return commandExecutor.writeReactive(getName(), ByteArrayCodec.INSTANCE, RedisCommands.SET,
        getName(), toByteArrayReverse(bs));
  }

  @Override
  public Publisher<Void> not() {
    return op("NOT");
  }

  @Override
  public Publisher<Void> set(long fromIndex, long toIndex) {
    CommandBatchService executorService =
        new CommandBatchService(commandExecutor.getConnectionManager());
    for (long i = fromIndex; i < toIndex; i++) {
      executorService.writeAsync(getName(), codec, RedisCommands.SETBIT_VOID, getName(), i, 1);
    }
    return new NettyFuturePublisher<Void>(executorService.executeAsyncVoid());
  }

  @Override
  public Publisher<Integer> size() {
    return commandExecutor.readReactive(getName(), codec, RedisCommands.BITS_SIZE, getName());
  }

  @Override
  public Publisher<Void> set(long bitIndex) {
    return set(bitIndex, true);
  }

  @Override
  public Publisher<Long> cardinality() {
    return commandExecutor.readReactive(getName(), codec, RedisCommands.BITCOUNT, getName());
  }

  @Override
  public Publisher<Void> clear(long bitIndex) {
    return set(bitIndex, false);
  }

  @Override
  public Publisher<Void> clear() {
    return commandExecutor.writeReactive(getName(), RedisCommands.DEL_VOID, getName());
  }

  @Override
  public Publisher<Void> or(String... bitSetNames) {
    return op("OR", bitSetNames);
  }

  @Override
  public Publisher<Void> and(String... bitSetNames) {
    return op("AND", bitSetNames);
  }

  @Override
  public Publisher<Void> xor(String... bitSetNames) {
    return op("XOR", bitSetNames);
  }

  @Override
  public String toString() {
    return Streams.create(asBitSet()).next().poll().toString();
  }

}
