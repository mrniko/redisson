/**
 * Copyright (c) 2013-2020 Nikita Koksharov
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
package org.redisson;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.executor.FairLockCache;
import org.redisson.pubsub.LockPubSub;

/**
 * Distributed implementation of {@link java.util.concurrent.locks.Lock}
 * Implements reentrant lock.<br>
 * Lock will be removed automatically if client disconnects.
 * <p>
 * Implements a <b>fair</b> locking so it guarantees an acquire order by threads.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonFairLock extends RedissonLock implements RLock {

    private final long threadWaitTime;
    private final CommandAsyncExecutor commandExecutor;
    private final String threadsQueueName;
    private final String timeoutSetName;
    public static final String registeredClientPrefix = "redisson_lock_clients";
    private FairLockCache fairLockCache;
    public static final long DEFAULT_THREAD_WAIT_TIME = 60000*5;

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name) {
        this(commandExecutor, name, DEFAULT_THREAD_WAIT_TIME);
    }

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name, FairLockCache fairLockCache) {
        this(commandExecutor, name, DEFAULT_THREAD_WAIT_TIME);
        this.fairLockCache = fairLockCache;
    }

    public RedissonFairLock(CommandAsyncExecutor commandExecutor, String name, long threadWaitTime) {
        super(commandExecutor, name);
        this.commandExecutor = commandExecutor;
        this.threadWaitTime = threadWaitTime;
        threadsQueueName = prefixName("redisson_lock_queue", name);
        timeoutSetName = prefixName("redisson_lock_timeout", name);
    }

    @Override
    protected RFuture<RedissonLockEntry> subscribe(long threadId) {
        return pubSub.subscribe(getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected void unsubscribe(RFuture<RedissonLockEntry> future, long threadId) {
        pubSub.unsubscribe(future.getNow(), getEntryName() + ":" + threadId,
                getChannelName() + ":" + getLockName(threadId));
    }

    @Override
    protected RFuture<Void> acquireFailedAsync(long waitTime, TimeUnit unit, long threadId) {
        long wait = threadWaitTime;
        if (waitTime != -1) {
            wait = unit.toMillis(waitTime);
        }

        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_VOID,
                // get the existing timeout for the thread to remove
                "local queue = redis.call('lrange', KEYS[1], 0, -1);" +
                // find the location in the queue where the thread is
                "local i = 1;" +
                "while i <= #queue and queue[i] ~= ARGV[1] do " +
                    "i = i + 1;" +
                "end;" +
                // go to the next index which will exist after the current thread is removed
                "i = i + 1;" +
                // decrement the timeout for the rest of the queue after the thread being removed
                "while i <= #queue do " +
                    "redis.call('zincrby', KEYS[2], -tonumber(ARGV[2]), queue[i]);" +
                    "i = i + 1;" +
                "end;" +
                // remove the thread from the queue and timeouts set
                "redis.call('zrem', KEYS[2], ARGV[1]);" +
                "redis.call('lrem', KEYS[1], 0, ARGV[1]);",
                Arrays.<Object>asList(threadsQueueName, timeoutSetName),
                getLockName(threadId), wait);
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        internalLockLeaseTime = unit.toMillis(leaseTime);

        long wait = threadWaitTime;
        if (waitTime != -1) {
            wait = unit.toMillis(waitTime);
        }

        long currentTime = System.currentTimeMillis();
        if (command == RedisCommands.EVAL_NULL_BOOLEAN) {
            return evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                    // remove stale threads
                    "while true do " +
                        "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                        "if firstThreadId2 == false then " +
                            "break;" +
                        "end;" +
                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                        "if timeout <= tonumber(ARGV[3]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            "redis.call('lpop', KEYS[2]);" +
                        "else " +
                            "break;" +
                        "end;" +
                    "end;" +

                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +
                        "redis.call('lpop', KEYS[2]);" +
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                        "for i = 1, #keys, 1 do " +
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[4]), keys[i]);" +
                        "end;" +

                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +
                    "if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +
                    "return 1;",
                    Arrays.asList(getName(), threadsQueueName, timeoutSetName),
                    internalLockLeaseTime, getLockName(threadId), currentTime, wait);
        }

        String isClientRegistered = "1";
        String clientQueuePrefix = "";
        String clientElementName = "";
        long clientElementTtl = 0L;
        if (null != fairLockCache) {
            isClientRegistered = fairLockCache.isLockRegistered(name) ? "1" : "0";
            clientQueuePrefix = fairLockCache.getClientQueueName(name) + "*";
            clientElementName = fairLockCache.getClientQueueElementName(name);
            clientElementTtl = fairLockCache.getTtl();
        }

        if (command == RedisCommands.EVAL_LONG) {
            RFuture<T> result = evalWriteAsync(getName(), LongCodec.INSTANCE, command,
                    // remove unnecessary lock queue
                    "if tonumber(ARGV[5]) == 0 then " +
                                "local clientNo = 0;" +
                                // only delete elements if there is no registered clients
                                "local registeredClients = redis.call('keys', KEYS[4]);" +
                                "if #registeredClients == 0 then " +
                                    "while true do " +
                                            "local firstThreadId = redis.call('lindex', KEYS[2], 0);" +
                                            "if firstThreadId == false then " +
                                                "break;" +
                                            "end;" +

                                                "redis.call('zrem', KEYS[3], firstThreadId);" +
                                                "redis.call('lpop', KEYS[2]);" +
                                    "end; " +

                                    // register client ttl
                                    "if (redis.call('exists', KEYS[5]) == 0) then " +
                                        "redis.call('set', KEYS[5], 1);" +
                                        "redis.call('pexpire', KEYS[5], ARGV[6]);" +
                                    "end;" +
                                "end;" +
                            "end;" +

                        // remove stale threads
                        "while true do " +
                            "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);" +
                            "if firstThreadId2 == false then " +
                            "break;" +
                        "end;" +

                        "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));" +
                        "if timeout <= tonumber(ARGV[4]) then " +
                            // remove the item from the queue and timeout set
                            // NOTE we do not alter any other timeout
                            "redis.call('zrem', KEYS[3], firstThreadId2);" +
                            "redis.call('lpop', KEYS[2]);" +
                        "else " +
                            "break;" +
                        "end;" +
                    "end;" +

                    // check if the lock can be acquired now
                    "if (redis.call('exists', KEYS[1]) == 0) " +
                        "and ((redis.call('exists', KEYS[2]) == 0) " +
                            "or (redis.call('lindex', KEYS[2], 0) == ARGV[2])) then " +

                        // remove this thread from the queue and timeout set
                        "redis.call('lpop', KEYS[2]);" +
                        "redis.call('zrem', KEYS[3], ARGV[2]);" +

                        // decrease timeouts for all waiting in the queue
                        "local keys = redis.call('zrange', KEYS[3], 0, -1);" +
                        "for i = 1, #keys, 1 do " +
                            "redis.call('zincrby', KEYS[3], -tonumber(ARGV[3]), keys[i]);" +
                        "end;" +

                        // acquire the lock and set the TTL for the lease
                        "redis.call('hset', KEYS[1], ARGV[2], 1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +

                    // check if the lock is already held, and this is a re-entry
                    "if redis.call('hexists', KEYS[1], ARGV[2]) == 1 then " +
                        "redis.call('hincrby', KEYS[1], ARGV[2],1);" +
                        "redis.call('pexpire', KEYS[1], ARGV[1]);" +
                        "return nil;" +
                    "end;" +

                    // the lock cannot be acquired
                    // check if the thread is already in the queue
                    "local timeout = redis.call('zscore', KEYS[3], ARGV[2]);" +
                    "if timeout ~= false then " +
                        // the real timeout is the timeout of the prior thread
                        // in the queue, but this is approximately correct, and
                        // avoids having to traverse the queue
                        "return timeout - tonumber(ARGV[3]) - tonumber(ARGV[4]);" +
                    "end;" +

                    // add the thread to the queue at the end, and set its timeout in the timeout set to the timeout of
                    // the prior thread in the queue (or the timeout of the lock if the queue is empty) plus the
                    // threadWaitTime
                    "local lastThreadId = redis.call('lindex', KEYS[2], -1);" +
                    "local ttl;" +
                    "if lastThreadId ~= false and lastThreadId ~= ARGV[2] then " +
                        "ttl = tonumber(redis.call('zscore', KEYS[3], lastThreadId)) - tonumber(ARGV[4]);" +
                    "else " +
                        "ttl = redis.call('pttl', KEYS[1]);" +
                    "end;" +
                    "local timeout = ttl + tonumber(ARGV[3]) + tonumber(ARGV[4]);" +
                    "if redis.call('zadd', KEYS[3], timeout, ARGV[2]) == 1 then " +
                        "redis.call('rpush', KEYS[2], ARGV[2]);" +
                    "end;" +
                    "return ttl;",
                    Arrays.asList(getName(), threadsQueueName, timeoutSetName),
                    internalLockLeaseTime, getLockName(threadId), wait, currentTime);
        }

        throw new IllegalArgumentException();
    }

    @Override
    protected RFuture<Boolean> unlockInnerAsync(long threadId) {
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[4]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                
              + "if (redis.call('exists', KEYS[1]) == 0) then " + 
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " +
                    "return 1; " +
                "end;" +
                "if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then " +
                    "return nil;" +
                "end; " +
                "local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); " +
                "if (counter > 0) then " +
                    "redis.call('pexpire', KEYS[1], ARGV[2]); " +
                    "return 0; " +
                "end; " +
                    
                "redis.call('del', KEYS[1]); " +
                "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                "if nextThreadId ~= false then " +
                    "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                "end; " +
                "return 1; ",
                Arrays.asList(getName(), threadsQueueName, timeoutSetName, getChannelName()),
                LockPubSub.UNLOCK_MESSAGE, internalLockLeaseTime, getLockName(threadId), System.currentTimeMillis());
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return deleteAsync(getName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        List<Object> keys = Arrays.asList(getName(), threadsQueueName, timeoutSetName);
        return super.sizeInMemoryAsync(keys);
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit) {
        return expireAsync(timeToLive, timeUnit, getName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Boolean> expireAtAsync(long timestamp) {
        return expireAtAsync(timestamp, getName(), threadsQueueName, timeoutSetName);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return clearExpireAsync(getName(), threadsQueueName, timeoutSetName);
    }

    
    @Override
    public RFuture<Boolean> forceUnlockAsync() {
        cancelExpirationRenewal(null);
        return evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                // remove stale threads
                "while true do "
                + "local firstThreadId2 = redis.call('lindex', KEYS[2], 0);"
                + "if firstThreadId2 == false then "
                    + "break;"
                + "end; "
                + "local timeout = tonumber(redis.call('zscore', KEYS[3], firstThreadId2));"
                + "if timeout <= tonumber(ARGV[2]) then "
                    + "redis.call('zrem', KEYS[3], firstThreadId2); "
                    + "redis.call('lpop', KEYS[2]); "
                + "else "
                    + "break;"
                + "end; "
              + "end;"
                + 
                
                "if (redis.call('del', KEYS[1]) == 1) then " + 
                    "local nextThreadId = redis.call('lindex', KEYS[2], 0); " + 
                    "if nextThreadId ~= false then " +
                        "redis.call('publish', KEYS[4] .. ':' .. nextThreadId, ARGV[1]); " +
                    "end; " + 
                    "return 1; " + 
                "end; " + 
                "return 0;",
                Arrays.<Object>asList(getName(), threadsQueueName, timeoutSetName, getChannelName()), 
                LockPubSub.UNLOCK_MESSAGE, System.currentTimeMillis());
    }

    public FairLockCache getFairLockCache() {
        return fairLockCache;
    }

    public void setFairLockCache(FairLockCache fairLockCache) {
        this.fairLockCache = fairLockCache;
    }
}