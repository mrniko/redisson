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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.redisson.client.RedisException;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.decoder.ListScanResult;
import org.redisson.cluster.ClusterSlotRange;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.command.CommandBatchService;
import org.redisson.core.RKeys;
import org.redisson.misc.CompositeIterable;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

public class RedissonKeys implements RKeys {

  private final CommandAsyncExecutor commandExecutor;

  public RedissonKeys(CommandAsyncExecutor commandExecutor) {
    super();
    this.commandExecutor = commandExecutor;
  }

  @Override
  public int getSlot(String key) {
    return commandExecutor.get(getSlotAsync(key));
  }

  @Override
  public Future<Integer> getSlotAsync(String key) {
    return commandExecutor.readAsync(null, RedisCommands.KEYSLOT, key);
  }

  @Override
  public Iterable<String> getKeysByPattern(final String pattern) {
    List<Iterable<String>> iterables = new ArrayList<Iterable<String>>();
    for (final ClusterSlotRange slot : commandExecutor.getConnectionManager().getEntries().keySet()) {
      Iterable<String> iterable = new Iterable<String>() {
        @Override
        public Iterator<String> iterator() {
          return createKeysIterator(slot.getStartSlot(), pattern);
        }
      };
      iterables.add(iterable);
    }
    return new CompositeIterable<String>(iterables);
  }

  @Override
  public Iterable<String> getKeys() {
    return getKeysByPattern(null);
  }

  private ListScanResult<String> scanIterator(int slot, long startPos, String pattern) {
    if (pattern == null) {
      Future<ListScanResult<String>> f =
          commandExecutor.writeAsync(slot, StringCodec.INSTANCE, RedisCommands.SCAN, startPos);
      return commandExecutor.get(f);
    }
    Future<ListScanResult<String>> f =
        commandExecutor.writeAsync(slot, StringCodec.INSTANCE, RedisCommands.SCAN, startPos,
            "MATCH", pattern);
    return commandExecutor.get(f);
  }

  private Iterator<String> createKeysIterator(final int slot, final String pattern) {
    return new Iterator<String>() {

      private List<String> firstValues;
      private Iterator<String> iter;
      private long iterPos;

      private boolean removeExecuted;
      private String value;

      @Override
      public boolean hasNext() {
        if (iter == null || !iter.hasNext()) {
          ListScanResult<String> res = scanIterator(slot, iterPos, pattern);
          if (iterPos == 0 && firstValues == null) {
            firstValues = res.getValues();
          } else if (res.getValues().equals(firstValues)) {
            return false;
          }
          iter = res.getValues().iterator();
          iterPos = res.getPos();
        }
        return iter.hasNext();
      }

      @Override
      public String next() {
        if (!hasNext()) {
          throw new NoSuchElementException("No such element");
        }

        value = iter.next();
        removeExecuted = false;
        return value;
      }

      @Override
      public void remove() {
        if (removeExecuted) {
          throw new IllegalStateException("Element been already deleted");
        }
        if (iter == null) {
          throw new IllegalStateException();
        }

        iter.remove();
        delete(value);
        removeExecuted = true;
      }

    };
  }

  @Override
  public String randomKey() {
    return commandExecutor.get(randomKeyAsync());
  }

  @Override
  public Future<String> randomKeyAsync() {
    return commandExecutor.readRandomAsync(RedisCommands.RANDOM_KEY);
  }

  @Override
  public Collection<String> findKeysByPattern(String pattern) {
    return commandExecutor.get(findKeysByPatternAsync(pattern));
  }

  @Override
  public Future<Collection<String>> findKeysByPatternAsync(String pattern) {
    return commandExecutor.readAllAsync(RedisCommands.KEYS, pattern);
  }

  @Override
  public long deleteByPattern(String pattern) {
    return commandExecutor.get(deleteByPatternAsync(pattern));
  }

  @Override
  public Future<Long> deleteByPatternAsync(String pattern) {
    if (!commandExecutor.getConnectionManager().isClusterMode()) {
      return commandExecutor.evalWriteAsync((String) null, null, RedisCommands.EVAL_LONG,
          "local keys = redis.call('keys', ARGV[1]) " + "local n = 0 " + "for i=1, #keys,5000 do "
              + "n = n + redis.call('del', unpack(keys, i, math.min(i+4999, table.getn(keys)))) "
              + "end " + "return n;", Collections.emptyList(), pattern);
    }

    final Promise<Long> result = commandExecutor.getConnectionManager().newPromise();
    final AtomicReference<Throwable> failed = new AtomicReference<Throwable>();
    final AtomicLong count = new AtomicLong();
    final AtomicLong executed =
        new AtomicLong(commandExecutor.getConnectionManager().getEntries().size());
    final FutureListener<Long> listener = new FutureListener<Long>() {
      @Override
      public void operationComplete(Future<Long> future) throws Exception {
        if (future.isSuccess()) {
          count.addAndGet(future.getNow());
        } else {
          failed.set(future.cause());
        }

        checkExecution(result, failed, count, executed);
      }
    };

    for (ClusterSlotRange slot : commandExecutor.getConnectionManager().getEntries().keySet()) {
      Future<Collection<String>> findFuture =
          commandExecutor.readAsync(slot.getStartSlot(), null, RedisCommands.KEYS, pattern);
      findFuture.addListener(new FutureListener<Collection<String>>() {
        @Override
        public void operationComplete(Future<Collection<String>> future) throws Exception {
          if (!future.isSuccess()) {
            failed.set(future.cause());
            checkExecution(result, failed, count, executed);
            return;
          }

          Collection<String> keys = future.getNow();
          if (keys.isEmpty()) {
            checkExecution(result, failed, count, executed);
            return;
          }

          Future<Long> deleteFuture = deleteAsync(keys.toArray(new String[keys.size()]));
          deleteFuture.addListener(listener);
        }
      });
    }

    return result;
  }

  @Override
  public long delete(String... keys) {
    return commandExecutor.get(deleteAsync(keys));
  }

  @Override
  public Future<Long> deleteAsync(String... keys) {
    if (!commandExecutor.getConnectionManager().isClusterMode()) {
      return commandExecutor.writeAsync(null, RedisCommands.DEL, keys);
    }

    Map<ClusterSlotRange, List<String>> range2key = new HashMap<ClusterSlotRange, List<String>>();
    for (String key : keys) {
      int slot = commandExecutor.getConnectionManager().calcSlot(key);
      for (ClusterSlotRange range : commandExecutor.getConnectionManager().getEntries().keySet()) {
        if (range.isOwn(slot)) {
          List<String> list = range2key.get(range);
          if (list == null) {
            list = new ArrayList<String>();
            range2key.put(range, list);
          }
          list.add(key);
        }
      }
    }

    final Promise<Long> result = commandExecutor.getConnectionManager().newPromise();
    final AtomicReference<Throwable> failed = new AtomicReference<Throwable>();
    final AtomicLong count = new AtomicLong();
    final AtomicLong executed = new AtomicLong(range2key.size());
    FutureListener<List<?>> listener = new FutureListener<List<?>>() {
      @Override
      public void operationComplete(Future<List<?>> future) throws Exception {
        if (future.isSuccess()) {
          List<Long> result = (List<Long>) future.get();
          for (Long res : result) {
            count.addAndGet(res);
          }
        } else {
          failed.set(future.cause());
        }

        checkExecution(result, failed, count, executed);
      }
    };

    for (Entry<ClusterSlotRange, List<String>> entry : range2key.entrySet()) {
      // executes in batch due to CROSSLOT error
      CommandBatchService executorService =
          new CommandBatchService(commandExecutor.getConnectionManager());
      for (String key : entry.getValue()) {
        executorService.writeAsync(entry.getKey().getStartSlot(), null, RedisCommands.DEL, key);
      }

      Future<List<?>> future = executorService.executeAsync();
      future.addListener(listener);
    }

    return result;
  }

  @Override
  public Long count() {
    return commandExecutor.get(countAsync());
  }

  @Override
  public Future<Long> countAsync() {
    return commandExecutor.readAllAsync(RedisCommands.DBSIZE, new SlotCallback<Long, Long>() {
      AtomicLong results = new AtomicLong();

      @Override
      public void onSlotResult(Long result) {
        results.addAndGet(result);
      }

      @Override
      public Long onFinish() {
        return results.get();
      }
    });
  }

  @Override
  public void flushdb() {
    commandExecutor.get(flushdbAsync());
  }

  @Override
  public Future<Void> flushdbAsync() {
    return commandExecutor.writeAllAsync(RedisCommands.FLUSHDB);
  }

  @Override
  public void flushall() {
    commandExecutor.get(flushallAsync());
  }

  @Override
  public Future<Void> flushallAsync() {
    return commandExecutor.writeAllAsync(RedisCommands.FLUSHALL);
  }

  private void checkExecution(final Promise<Long> result, final AtomicReference<Throwable> failed,
      final AtomicLong count, final AtomicLong executed) {
    if (executed.decrementAndGet() == 0) {
      if (failed.get() != null) {
        if (count.get() > 0) {
          RedisException ex =
              new RedisException("" + count.get()
                  + " keys has been deleted. But one or more nodes has an error", failed.get());
          result.setFailure(ex);
        } else {
          result.setFailure(failed.get());
        }
      } else {
        result.setSuccess(count.get());
      }
    }
  }

}
