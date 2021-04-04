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
package org.redisson;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.redisson.api.*;
import org.redisson.api.listener.MessageListener;
import org.redisson.api.map.event.EntryCreatedListener;
import org.redisson.api.map.event.EntryEvent;
import org.redisson.api.map.event.EntryExpiredListener;
import org.redisson.api.map.event.EntryRemovedListener;
import org.redisson.api.map.event.EntryUpdatedListener;
import org.redisson.api.map.event.MapEntryListener;
import org.redisson.client.RedisClient;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.NumberConvertor;
import org.redisson.client.protocol.decoder.*;
import org.redisson.codec.BaseEventCodec;
import org.redisson.codec.MapCacheEventCodec;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.connection.decoder.MapGetAllDecoder;
import org.redisson.eviction.EvictionScheduler;
import org.redisson.misc.RedissonPromise;

import io.netty.buffer.ByteBuf;

/**
 * <p>Map-based cache with ability to set TTL for each entry via
 * {@link #put(Object, Object, long, TimeUnit)} or {@link #putIfAbsent(Object, Object, long, TimeUnit)} methods.
 * And therefore has an complex lua-scripts inside.</p>
 *
 * <p>Current redis implementation doesnt have map entry eviction functionality.
 * Thus entries are checked for TTL expiration during any key/value/entry read operation.
 * If key/value/entry expired then it doesn't returns and clean task runs asynchronous.
 * Clean task deletes removes 100 expired entries at once.
 * In addition there is {@link EvictionScheduler}. This scheduler
 * deletes expired entries in time interval between 5 seconds to 2 hours.</p>
 *
 * <p>If eviction is not required then it's better to use {@link RedissonMap} object.</p>
 *
 * @author Nikita Koksharov
 *
 * @param <K> key
 * @param <V> value
 */
public class RedissonMapCache<K, V> extends RedissonMap<K, V> implements RMapCache<K, V> {

    private EvictionScheduler evictionScheduler;
    private long defaultTTL = 0;

    public RedissonMapCache(EvictionScheduler evictionScheduler, CommandAsyncExecutor commandExecutor,
                            String name, RedissonClient redisson, MapOptions<K, V> options, WriteBehindService writeBehindService) {
        super(commandExecutor, name, redisson, options, writeBehindService);
        if (evictionScheduler != null) {
            evictionScheduler.schedule(getName(), getTimeoutSetName(), getIdleSetName(), getExpiredChannelName(), getLastAccessTimeSetName());
        }
        this.evictionScheduler = evictionScheduler;
    }

    public RedissonMapCache(Codec codec, EvictionScheduler evictionScheduler, CommandAsyncExecutor commandExecutor,
                            String name, RedissonClient redisson, MapOptions<K, V> options, WriteBehindService writeBehindService) {
        super(codec, commandExecutor, name, redisson, options, writeBehindService);
        if (evictionScheduler != null) {
            evictionScheduler.schedule(getName(), getTimeoutSetName(), getIdleSetName(), getExpiredChannelName(), getLastAccessTimeSetName());
        }
        this.evictionScheduler = evictionScheduler;
    }

    @Override
    public boolean trySetMaxSize(int maxSize) {
        return get(trySetMaxSizeAsync(maxSize));
    }

    public boolean trySetMaxSize(int maxSize, EvictionMode mode) {
        return get(trySetMaxSizeAsync(maxSize, mode));
    }

    @Override
    public RFuture<Boolean> trySetMaxSizeAsync(int maxSize) {
        return trySetMaxSizeAsync(maxSize, EvictionMode.LRU);
    }
    
    public RFuture<Boolean> trySetMaxSizeAsync(int maxSize, EvictionMode mode) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize should be greater than zero");
        }

        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "redis.call('hsetnx', KEYS[1], 'max-size', ARGV[1]);"
              + "return redis.call('hsetnx', KEYS[1], 'mode', ARGV[2]);",
                Collections.singletonList(getOptionsName()), maxSize, mode);
    }

    @Override
    public void setMaxSize(int maxSize) {
        get(setMaxSizeAsync(maxSize));
    }

    public void setMaxSize(int maxSize, EvictionMode mode) {
        get(setMaxSizeAsync(maxSize, mode));
    }

    @Override
    public RFuture<Void> setMaxSizeAsync(int maxSize) {
        return setMaxSizeAsync(maxSize, EvictionMode.LRU);
    }

    public RFuture<Void> setMaxSizeAsync(int maxSize, EvictionMode mode) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize should be greater than zero");
        }

        List<Object> params = new ArrayList<>(3);
        params.add(getOptionsName());
        params.add("max-size");
        params.add(maxSize);
        params.add("mode");
        params.add(mode);

        return commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, RedisCommands.HMSET, params.toArray());
    }

    @Override
    public RFuture<Boolean> containsKeyAsync(Object key) {
        checkKey(key);

        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_BOOLEAN,
        "local value = redis.call('hget', KEYS[1], ARGV[2]); " +
                "local expireDate = 92233720368547758; " +
                "if value ~= false then " +

                "    local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size')); " +
                "    if maxSize ~= nil and maxSize ~= 0 then " +
                "        local mode = redis.call('hget', KEYS[5], 'mode'); " +
                "        if mode == false or mode == 'LRU' then " +
                "               redis.call('zadd', KEYS[4], tonumber(ARGV[1]), ARGV[2]); " +
                "        else " +
                "               redis.call('zincrby', KEYS[4], 1, ARGV[2]); " +
                "        end; " +
                "    end;" +

                "    local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); " +
                "    if expireDateScore ~= false then " +
                "        expireDate = tonumber(expireDateScore) " +
                "    end; " +
                "    local t, val = struct.unpack('dLc0', value); " +
                "    if t ~= 0 then " +
                "        local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); " +
                "        if expireIdle ~= false then " +
                "            if tonumber(expireIdle) > tonumber(ARGV[1]) then " +
                "                redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), ARGV[2]); " +
                "            end ;" +
                "            expireDate = math.min(expireDate, tonumber(expireIdle)) " +
                "        end; " +
                "    end; " +
                "    if expireDate <= tonumber(ARGV[1]) then " +
                "        return 0; " +
                "    end; " +
                "    return 1;" +
                "end;" +
                "return 0; ",
                Arrays.<Object>asList(name, getTimeoutSetName(name), getIdleSetName(name), getLastAccessTimeSetName(name), getOptionsName(name)),
                System.currentTimeMillis(), encodeMapKey(key));
    }

    @Override
    public RFuture<Boolean> containsValueAsync(Object value) {
        checkValue(value);

        return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_BOOLEAN,
        "local s = redis.call('hgetall', KEYS[1]); " +
                "for i, v in ipairs(s) do " +
                "    if i % 2 == 0 then " +
                "        local t, val = struct.unpack('dLc0', v); " +
                "        if ARGV[2] == val then " +
                "            local key = s[i - 1]; " +

                "            local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size')); " +
                "            if maxSize ~= nil and maxSize ~= 0 then " +
                "                local mode = redis.call('hget', KEYS[5], 'mode'); " +
                "                if mode == false or mode == 'LRU' then " +
                "                    redis.call('zadd', KEYS[4], tonumber(ARGV[1]), key); " +
                "                else " +
                "                    redis.call('zincrby', KEYS[4], 1, key); " +
                "                end; " +
                "            end; " +

                "            local expireDate = 92233720368547758; " +
                "            local expireDateScore = redis.call('zscore', KEYS[2], key); " +
                "            if expireDateScore ~= false then " +
                "                expireDate = tonumber(expireDateScore) " +
                "            end; " +
                "            if t ~= 0 then " +
                "                local expireIdle = redis.call('zscore', KEYS[3], key); " +
                "                if expireIdle ~= false then " +
                "                    if tonumber(expireIdle) > tonumber(ARGV[1]) then " +
                "                        redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), key); " +
                "                    end; " +
                "                    expireDate = math.min(expireDate, tonumber(expireIdle)) " +
                "                end; " +
                "            end; " +
                "            if expireDate <= tonumber(ARGV[1]) then " +
                "                return 0; " +
                "            end; " +
                "            return 1; " +
                "        end;" +
                "    end;" +
                "end;" +
                "return 0;",
                Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()),
                System.currentTimeMillis(), encodeMapValue(value));
    }

    @Override
    public RFuture<Map<K, V>> getAllOperationAsync(Set<K> keys) {
        List<Object> args = new ArrayList<Object>(keys.size() + 1);
        List<Object> plainKeys = new ArrayList<Object>(keys.size());
        
        args.add(System.currentTimeMillis());
        for (K key : keys) {
            plainKeys.add(key);
            args.add(encodeMapKey(key));
        }

        return commandExecutor.evalWriteAsync(getName(), codec, new RedisCommand<Map<Object, Object>>("EVAL",
                        new MapValueDecoder(new MapGetAllDecoder(plainKeys, 0))),
            "local expireHead = redis.call('zrange', KEYS[2], 0, 0, 'withscores'); " +
            "local currentTime = tonumber(table.remove(ARGV, 1)); " + // index is the first parameter
            "local hasExpire = #expireHead == 2 and tonumber(expireHead[2]) <= currentTime; " +
            "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size'));" +
            "local map = {}; " +
            "for i = 1, #ARGV, 1 do " +
            "    local value = redis.call('hget', KEYS[1], ARGV[i]); " + 
            "    map[i] = false;" +
            "    if value ~= false then " +
            "        local key = ARGV[i]; " +
            "        local t, val = struct.unpack('dLc0', value); " +
            "        map[i] = val; " +
            "        if maxSize ~= nil and maxSize ~= 0 then " +
            "                local mode = redis.call('hget', KEYS[5], 'mode'); " +
            "                if mode == false or mode == 'LRU' then " +
            "                    redis.call('zadd', KEYS[4], currentTime, key); " +
            "                else " +
            "                    redis.call('zincrby', KEYS[4], 1, key); " +
            "                end; " +
            "        end; " +
            "        if hasExpire then " +
            "            local expireDate = redis.call('zscore', KEYS[2], key); " +
            "            if expireDate ~= false and tonumber(expireDate) <= currentTime then " +
            "                map[i] = false; " +
            "            end; " +
            "        end; " +
            "        if t ~= 0 then " +
            "            local expireIdle = redis.call('zscore', KEYS[3], key); " +
            "            if expireIdle ~= false then " +
            "                if tonumber(expireIdle) > currentTime then " +
            "                    redis.call('zadd', KEYS[3], t + currentTime, key); " +
            "                else " +
            "                    map[i] = false; " +
            "                end; " +
            "            end; " +
            "        end; " +
            "    end; " +
            "end; " +
            "return map;",
            Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()), 
            args.toArray());
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit) {
        return get(putIfAbsentAsync(key, value, ttl, ttlUnit));
    }

    @Override
    public RFuture<V> putIfAbsentAsync(K key, V value, long ttl, TimeUnit ttlUnit) {
        return putIfAbsentAsync(key, value, ttl, ttlUnit, 0, null);
    }

    @Override
    public V putIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
        return get(putIfAbsentAsync(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
    }

    @Override
    public RFuture<V> putIfAbsentAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
        checkKey(key);
        checkValue(value);
        
        if (ttl < 0) {
            throw new IllegalArgumentException("ttl can't be negative");
        }
        if (maxIdleTime < 0) {
            throw new IllegalArgumentException("maxIdleTime can't be negative");
        }
        if (ttl == 0 && maxIdleTime == 0) {
            return putIfAbsentAsync(key, value);
        }

        if (ttl > 0 && ttlUnit == null) {
            throw new NullPointerException("ttlUnit param can't be null");
        }

        if (maxIdleTime > 0 && maxIdleUnit == null) {
            throw new NullPointerException("maxIdleUnit param can't be null");
        }

        long ttlTimeout = 0;
        if (ttl > 0) {
            ttlTimeout = System.currentTimeMillis() + ttlUnit.toMillis(ttl);
        }

        long maxIdleTimeout = 0;
        long maxIdleDelta = 0;
        if (maxIdleTime > 0) {
            maxIdleDelta = maxIdleUnit.toMillis(maxIdleTime);
            maxIdleTimeout = System.currentTimeMillis() + maxIdleDelta;
        }

        String name = getName(key);
        RFuture<V> future = commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_MAP_VALUE,
                "local insertable = false; "
                        + "local value = redis.call('hget', KEYS[1], ARGV[5]); "
                            + "if value == false then "
                            + "insertable = true; "
                        + "else "
                            + "local t, val = struct.unpack('dLc0', value); "
                            + "local expireDate = 92233720368547758; "
                            + "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[5]); "
                            + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                            + "end; "
                            + "if t ~= 0 then "
                                + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[5]); "
                                + "if expireIdle ~= false then "
                                    + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                + "end; "
                            + "end; "
                            + "if expireDate <= tonumber(ARGV[1]) then "
                                + "insertable = true; "
                            + "end; "
                        + "end; "

                        + "if insertable == true then "
                            // ttl
                            + "if tonumber(ARGV[2]) > 0 then "
                                + "redis.call('zadd', KEYS[2], ARGV[2], ARGV[5]); "
                            + "else "
                                + "redis.call('zrem', KEYS[2], ARGV[5]); "
                            + "end; "

                            // idle
                            + "if tonumber(ARGV[3]) > 0 then "
                                + "redis.call('zadd', KEYS[3], ARGV[3], ARGV[5]); "
                            + "else "
                                + "redis.call('zrem', KEYS[3], ARGV[5]); "
                            + "end; "

                            // last access time
                            + "local maxSize = tonumber(redis.call('hget', KEYS[7], 'max-size')); " +
                            "if maxSize ~= nil and maxSize ~= 0 then " +
                            "    local currentTime = tonumber(ARGV[1]); " +
                            "    local lastAccessTimeSetName = KEYS[5]; " +

                                "local mode = redis.call('hget', KEYS[7], 'mode'); " +
                                "if mode == false or mode == 'LRU' then " +
                                    "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[5]); " +
                                "end; " +

                            "    local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                            "    if cacheSize >= maxSize then " +
                            "        local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize); " +
                            "        for index, lruItem in ipairs(lruItems) do " +
                            "            if lruItem and lruItem ~= ARGV[5] then " +
                            "                local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                            "                redis.call('hdel', KEYS[1], lruItem); " +
                            "                redis.call('zrem', KEYS[2], lruItem); " +
                            "                redis.call('zrem', KEYS[3], lruItem); " +
                            "                redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                            "                if lruItemValue ~= false then " +
                                "                local removedChannelName = KEYS[6]; " +
                                                "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                    local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                                "                redis.call('publish', removedChannelName, msg); " +
                                             "end; " +
                            "            end; " +
                            "        end; " +
                            "    end; " +

                                "if mode == 'LFU' then " +
                                    "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[5]); " +
                                "end; " +

                            "end; "

                            // value
                            + "local val = struct.pack('dLc0', tonumber(ARGV[4]), string.len(ARGV[6]), ARGV[6]); "
                            + "redis.call('hset', KEYS[1], ARGV[5], val); "

                            + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[5]), ARGV[5], string.len(ARGV[6]), ARGV[6]); "
                            + "redis.call('publish', KEYS[4], msg); "

                            + "return nil; "
                        + "else "
                            + "local t, val = struct.unpack('dLc0', value); "
                            + "redis.call('zadd', KEYS[3], t + ARGV[1], ARGV[5]); "
                            + "return val; "
                        + "end; ",
                Arrays.<Object>asList(name, getTimeoutSetName(name), getIdleSetName(name), getCreatedChannelName(name),
                        getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), ttlTimeout, maxIdleTimeout, maxIdleDelta, encodeMapKey(key), encodeMapValue(value));
        if (hasNoWriter()) {
            return future;
        }
        MapWriterTask.Add task = new MapWriterTask.Add(key, value);
        return mapWriterFuture(future, task, r -> r == null);
    }

    @Override
    protected RFuture<Boolean> removeOperationAsync(Object key, Object value) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_BOOLEAN,
                "local value = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "if value == false then "
                            + "return 0; "
                        + "end; "
                        + "local t, val = struct.unpack('dLc0', value); "
                        + "local expireDate = 92233720368547758; " +
                        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); "
                        + "if expireDateScore ~= false then "
                            + "expireDate = tonumber(expireDateScore) "
                        + "end; "
                        + "if t ~= 0 then "
                            + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); "
                            + "if expireIdle ~= false then "
                                + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                            + "end; "
                        + "end; "
                        + "if expireDate <= tonumber(ARGV[1]) then "
                            + "return 0; "
                        + "end; "

                        + "if val == ARGV[3] then "
                            + "redis.call('zrem', KEYS[2], ARGV[2]); "
                            + "redis.call('zrem', KEYS[3], ARGV[2]); "
                            + "local maxSize = tonumber(redis.call('hget', KEYS[6], 'max-size')); " +
                                "if maxSize ~= nil and maxSize ~= 0 then " +
                                "   redis.call('zrem', KEYS[5], ARGV[2]); " +
                                "end; "
                            + "redis.call('hdel', KEYS[1], ARGV[2]); "
                            + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(val), val); "
                            + "redis.call('publish', KEYS[4], msg); "
                            + "return 1; "
                        + "else "
                            + "return 0; "
                        + "end",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getRemovedChannelName(name),
                        getLastAccessTimeSetName(name), getOptionsName(name)),
                System.currentTimeMillis(), encodeMapKey(key), encodeMapValue(value));
    }

    @Override
    public RFuture<V> getOperationAsync(K key) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_MAP_VALUE,
                "local value = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "if value == false then "
                            + "return nil; "
                        + "end; "
                        + "local t, val = struct.unpack('dLc0', value); "
                        + "local expireDate = 92233720368547758; " +
                        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); "
                        + "if expireDateScore ~= false then "
                            + "expireDate = tonumber(expireDateScore) "
                        + "end; "
                        + "if t ~= 0 then "
                            + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); "
                            + "if expireIdle ~= false then "
                                + "if tonumber(expireIdle) > tonumber(ARGV[1]) then "
                                    + "redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), ARGV[2]); "
                                + "end; "
                                + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                            + "end; "
                        + "end; "
                        + "if expireDate <= tonumber(ARGV[1]) then "
                            + "return nil; "
                        + "end; "
                        + "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size')); " +
                        "if maxSize ~= nil and maxSize ~= 0 then " +
                            "local mode = redis.call('hget', KEYS[5], 'mode'); " +
                            "if mode == false or mode == 'LRU' then " +
                                "redis.call('zadd', KEYS[4], tonumber(ARGV[1]), ARGV[2]); " +
                            "else " +
                                "redis.call('zincrby', KEYS[4], 1, ARGV[2]); " +
                            "end; " +
                        "end; "
                        + "return val; ",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getLastAccessTimeSetName(name), getOptionsName(name)),
                System.currentTimeMillis(), encodeMapKey(key));
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit unit) {
        return get(putAsync(key, value, ttl, unit));
    }

    @Override
    protected RFuture<V> putOperationAsync(K key, V value) {
        long ttlTimeout = 0;
        if (defaultTTL > 0) {
            ttlTimeout = System.currentTimeMillis() + defaultTTL;
        }
        return putOperationAsync(key, value, ttlTimeout, 0, 0, 0);
    }

    @Override
    protected RFuture<V> putIfExistsOperationAsync(K key, V value) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_MAP_VALUE,
                    "local value = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "if value == false then "
                            + "return nil;"
                        + "end; "
                        + "local maxSize = tonumber(redis.call('hget', KEYS[7], 'max-size'));"
                        + "local lastAccessTimeSetName = KEYS[5]; "
                        + "local currentTime = tonumber(ARGV[1]); "
                        + "local t, val;"
                        + "if value ~= false then "
                            + "t, val = struct.unpack('dLc0', value); "
                            + "local expireDate = 92233720368547758; "
                            + "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); "
                            + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                            + "end; "
                            + "if t ~= 0 then "
                                + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); "
                                + "if expireIdle ~= false then "
                                    + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                + "end; "
                            + "end; "
                            + "if expireDate > tonumber(ARGV[1]) then "
                                + "if maxSize ~= nil and maxSize ~= 0 then " +
                                    "local mode = redis.call('hget', KEYS[7], 'mode'); " +
                                    "if mode == false or mode == 'LRU' then " +
                                        "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[2]); " +
                                    "else " +
                                        "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[2]); " +
                                    "end; "
                                + "end; "
                            + "else "
                                + "return nil; "
                            + "end; "
                        + "end; "

                        + "local newValue = struct.pack('dLc0', 0, string.len(ARGV[3]), ARGV[3]); "
                        + "redis.call('hset', KEYS[1], ARGV[2], newValue); "

                        // last access time
                        + "if maxSize ~= nil and maxSize ~= 0 then " +
                            "local mode = redis.call('hget', KEYS[7], 'mode'); " +

                            "if mode == false or mode == 'LRU' then " +
                                "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[2]); " +
                            "end; " +

                        "    local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                        "    if cacheSize > maxSize then " +
                        "        local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize - 1); " +
                        "        for index, lruItem in ipairs(lruItems) do " +
                        "            if lruItem and lruItem ~= ARGV[2] then " +
                        "                local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                        "                redis.call('hdel', KEYS[1], lruItem); " +
                        "                redis.call('zrem', KEYS[2], lruItem); " +
                        "                redis.call('zrem', KEYS[3], lruItem); " +
                        "                redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                        "                if lruItemValue ~= false then " +
                            "                local removedChannelName = KEYS[6]; " +
                                            "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                            "                redis.call('publish', removedChannelName, msg); " +
                                        "end; " +
                        "            end; " +
                        "        end; " +
                        "    end; " +

                            "if mode == 'LFU' then " +
                                "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[2]); " +
                            "end; " +

                        "end; "

                        + "local msg = struct.pack('Lc0Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3], string.len(val), val); "
                        + "redis.call('publish', KEYS[4], msg); "
                        + "return val;",
                Arrays.<Object>asList(name, getTimeoutSetName(name), getIdleSetName(name), getUpdatedChannelName(name),
                        getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), encodeMapKey(key), encodeMapValue(value));
    }

    @Override
    protected RFuture<V> putIfAbsentOperationAsync(K key, V value) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_MAP_VALUE,
                "local value = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "local maxSize = tonumber(redis.call('hget', KEYS[7], 'max-size'));"
                        + "local lastAccessTimeSetName = KEYS[5]; "
                        + "local currentTime = tonumber(ARGV[1]); "
                        + "if value ~= false then "
                            + "local t, val = struct.unpack('dLc0', value); "
                            + "local expireDate = 92233720368547758; "
                            + "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); "
                            + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                            + "end; "
                            + "if t ~= 0 then "
                                + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); "
                                + "if expireIdle ~= false then "
                                    + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                + "end; "
                            + "end; "
                            + "if expireDate > tonumber(ARGV[1]) then "
                                + "if maxSize ~= nil and maxSize ~= 0 then " +
                                    "local mode = redis.call('hget', KEYS[7], 'mode'); " +
                                    "if mode == false or mode == 'LRU' then " +
                                        "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[2]); " +
                                    "else " +
                                        "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[2]); " +
                                    "end; "
                                + "end; "
                                + "return val; "
                            + "end; "
                        + "end; "

                        + "local value = struct.pack('dLc0', 0, string.len(ARGV[3]), ARGV[3]); "
                        + "redis.call('hset', KEYS[1], ARGV[2], value); "

                        // last access time
                        + "if maxSize ~= nil and maxSize ~= 0 then " +
                            "local mode = redis.call('hget', KEYS[7], 'mode'); " +

                            "if mode == false or mode == 'LRU' then " +
                                "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[2]); " +
                            "end; " +

                        "    local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                        "    if cacheSize > maxSize then " +
                        "        local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize - 1); " +
                        "        for index, lruItem in ipairs(lruItems) do " +
                        "            if lruItem and lruItem ~= ARGV[2] then " +
                        "                local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                        "                redis.call('hdel', KEYS[1], lruItem); " +
                        "                redis.call('zrem', KEYS[2], lruItem); " +
                        "                redis.call('zrem', KEYS[3], lruItem); " +
                        "                redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                        "                if lruItemValue ~= false then " +
                            "                local removedChannelName = KEYS[6]; " +
                                            "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                            "                redis.call('publish', removedChannelName, msg); " + 
                                        "end; " +
                        "            end; " +
                        "        end; " +
                        "    end; " +

                            "if mode == 'LFU' then " +
                                "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[2]); " +
                            "end; " +

                        "end; "

                        + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3]); "
                        + "redis.call('publish', KEYS[4], msg); "
                        + "return nil;",
                Arrays.<Object>asList(name, getTimeoutSetName(name), getIdleSetName(name), getCreatedChannelName(name),
                        getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), encodeMapKey(key), encodeMapValue(value));
    }
    
    @Override
    public void putAll(Map<? extends K, ? extends V> map, long ttl, TimeUnit ttlUnit) {
        get(putAllAsync(map, ttl, ttlUnit));
    }
    
    @Override
    public RFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, long ttl, TimeUnit ttlUnit) {
        if (map.isEmpty()) {
            return RedissonPromise.newSucceededFuture(null);
        }

        RFuture<Void> future = putAllOperationAsync(map, ttl, ttlUnit);
        if (hasNoWriter()) {
            return future;
        }

        MapWriterTask listener = new MapWriterTask.Add(map);
        return mapWriterFuture(future, listener);
    }

    @Override
    public V addAndGet(K key, Number value) {
        return get(addAndGetAsync(key, value));
    }

    @Override
    protected RFuture<V> addAndGetOperationAsync(K key, Number value) {
        ByteBuf keyState = encodeMapKey(key);
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, StringCodec.INSTANCE,
                new RedisCommand<Object>("EVAL", new NumberConvertor(value.getClass())),
                "local value = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "local expireDate = 92233720368547758; "
                        + "local t = 0; "
                        + "local val = 0; "
                        + "if value ~= false then "
                            + "t, val = struct.unpack('dLc0', value); "
                            + "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); "
                            + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                            + "end; "
                            + "if t ~= 0 then "
                                + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); "
                                + "if expireIdle ~= false then "
                                    + "if tonumber(expireIdle) > tonumber(ARGV[1]) then "
                                        + "redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), ARGV[2]); "
                                    + "end; "
                                    + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                + "end; "
                            + "end; "
                        + "end; "

                        + "local newValue = tonumber(ARGV[3]); "
                        + "if value ~= false and expireDate > tonumber(ARGV[1]) then "
                            + "newValue = tonumber(val) + newValue; "

                            + "local msg = struct.pack('Lc0Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(newValue), newValue, string.len(val), val); "
                            + "redis.call('publish', KEYS[5], msg); "
                        + "else "
                            + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3]); "
                            + "redis.call('publish', KEYS[4], msg); "
                        + "end; "
                        + "local newValuePack = struct.pack('dLc0', t, string.len(newValue), newValue); "
                        + "redis.call('hset', KEYS[1], ARGV[2], newValuePack); "

                        // last access time
                        + "local maxSize = tonumber(redis.call('hget', KEYS[8], 'max-size')); " +
                        "if maxSize ~= nil and maxSize ~= 0 then " +
                        "    local currentTime = tonumber(ARGV[1]); " +
                        "    local lastAccessTimeSetName = KEYS[6]; " +

                            "local mode = redis.call('hget', KEYS[8], 'mode'); " +

                            "if mode == false or mode == 'LRU' then " +
                                "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[2]); " +
                            "end; " +

                        "    local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                        "    if cacheSize > maxSize then " +
                        "        local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize - 1); " +
                        "        for index, lruItem in ipairs(lruItems) do " +
                        "            if lruItem and lruItem ~= ARGV[2] then " +
                        "                local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                        "                redis.call('hdel', KEYS[1], lruItem); " +
                        "                redis.call('zrem', KEYS[2], lruItem); " +
                        "                redis.call('zrem', KEYS[3], lruItem); " +
                        "                redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                        "                if lruItemValue ~= false then " +                        
                            "                local removedChannelName = KEYS[7]; " +
                                            "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                            "                redis.call('publish', removedChannelName, msg); " +
                                        "end; " +
                        "            end; " +
                        "        end; " +
                        "    end; " +

                            "if mode == 'LFU' then " +
                                "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[2]); " +
                            "end; " +

                        "end; "

                        + "return tostring(newValue); ",
                Arrays.<Object>asList(name, getTimeoutSetName(name), getIdleSetName(name), getCreatedChannelName(name),
                        getUpdatedChannelName(name), getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), keyState, new BigDecimal(value.toString()).toPlainString());
    }

    @Override
    public boolean fastPut(K key, V value, long ttl, TimeUnit ttlUnit) {
        return get(fastPutAsync(key, value, ttl, ttlUnit));
    }

    @Override
    public RFuture<Boolean> fastPutAsync(K key, V value, long ttl, TimeUnit ttlUnit) {
        return fastPutAsync(key, value, ttl, ttlUnit, 0, null);
    }

    @Override
    public boolean fastPut(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
        return get(fastPutAsync(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
    }

    @Override
    public RFuture<Boolean> fastPutAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
        checkKey(key);
        checkValue(value);

        if (ttl < 0) {
            throw new IllegalArgumentException("ttl can't be negative");
        }
        if (maxIdleTime < 0) {
            throw new IllegalArgumentException("maxIdleTime can't be negative");
        }
        if (ttl == 0 && maxIdleTime == 0) {
            return fastPutAsync(key, value);
        }

        if (ttl > 0 && ttlUnit == null) {
            throw new NullPointerException("ttlUnit param can't be null");
        }

        if (maxIdleTime > 0 && maxIdleUnit == null) {
            throw new NullPointerException("maxIdleUnit param can't be null");
        }

        RFuture<Boolean> future = fastPutOperationAsync(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit);

        if (hasNoWriter()) {
            return future;
        }

        return mapWriterFuture(future, new MapWriterTask.Add(key, value));
    }

    protected RFuture<Boolean> fastPutOperationAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
        long currentTime = System.currentTimeMillis();
        long ttlTimeout = 0;
        if (ttl > 0) {
            ttlTimeout = currentTime + ttlUnit.toMillis(ttl);
        }

        long maxIdleTimeout = 0;
        long maxIdleDelta = 0;
        if (maxIdleTime > 0) {
            maxIdleDelta = maxIdleUnit.toMillis(maxIdleTime);
            maxIdleTimeout = currentTime + maxIdleDelta;
        }

        String name = getName(key);
        RFuture<Boolean> future = commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_BOOLEAN,
                "local insertable = false; "
                        + "local value = redis.call('hget', KEYS[1], ARGV[5]); "
                        + "local t, val;"
                        + "if value == false then "
                            + "insertable = true; "
                        + "else "
                            + "t, val = struct.unpack('dLc0', value); "
                            + "local expireDate = 92233720368547758; "
                            + "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[5]); "
                            + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                            + "end; "
                            + "if t ~= 0 then "
                                + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[5]); "
                                + "if expireIdle ~= false then "
                                    + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                + "end; "
                            + "end; "
                            + "if expireDate <= tonumber(ARGV[1]) then "
                                + "insertable = true; "
                            + "end; "
                        + "end; " +

                        "if tonumber(ARGV[2]) > 0 then "
                            + "redis.call('zadd', KEYS[2], ARGV[2], ARGV[5]); "
                        + "else "
                            + "redis.call('zrem', KEYS[2], ARGV[5]); "
                        + "end; "
                        + "if tonumber(ARGV[3]) > 0 then "
                            + "redis.call('zadd', KEYS[3], ARGV[3], ARGV[5]); "
                        + "else "
                            + "redis.call('zrem', KEYS[3], ARGV[5]); "
                        + "end; " +

                        // last access time
                        "local maxSize = tonumber(redis.call('hget', KEYS[8], 'max-size')); " +
                        "local mode = redis.call('hget', KEYS[8], 'mode'); " +
                        "if maxSize ~= nil and maxSize ~= 0 then " +
                        "    local currentTime = tonumber(ARGV[1]); " +
                        "    local lastAccessTimeSetName = KEYS[6]; " +

                           "if mode == false or mode == 'LRU' then " +
                               "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[5]); " +
                           "end; " +

                        "    local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                        "    if cacheSize >= maxSize then " +
                        "        local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize); " +
                        "        for index, lruItem in ipairs(lruItems) do " +
                        "            if lruItem and lruItem ~= ARGV[5] then " +
                        "                local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                        "                redis.call('hdel', KEYS[1], lruItem); " +
                        "                redis.call('zrem', KEYS[2], lruItem); " +
                        "                redis.call('zrem', KEYS[3], lruItem); " +
                        "                redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                        "                if lruItemValue ~= false then " +
                            "                local removedChannelName = KEYS[7]; " +
                                            "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                            "                redis.call('publish', removedChannelName, msg); " +
                                        "end; " +
                        "            end; " +
                        "        end; " +
                        "    end; " +

                            "if mode == 'LFU' then " +
                                 "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[5]); " +
                            "end; " +
                        "end; "

                        + "local value = struct.pack('dLc0', ARGV[4], string.len(ARGV[6]), ARGV[6]); "
                        + "redis.call('hset', KEYS[1], ARGV[5], value); "
                        + "if insertable == true then "
                            + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[5]), ARGV[5], string.len(ARGV[6]), ARGV[6]); "
                            + "redis.call('publish', KEYS[4], msg); "
                            + "return 1;"
                        + "else "
                            + "local msg = struct.pack('Lc0Lc0Lc0', string.len(ARGV[5]), ARGV[5], string.len(ARGV[6]), ARGV[6], string.len(val), val); "
                            + "redis.call('publish', KEYS[5], msg); "
                            + "return 0;"
                        + "end;",
                Arrays.<Object>asList(name, getTimeoutSetName(name), getIdleSetName(name), getCreatedChannelName(name),
                        getUpdatedChannelName(name), getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), ttlTimeout, maxIdleTimeout, maxIdleDelta, encodeMapKey(key), encodeMapValue(value));
        return future;
    }

    @Override
    public RFuture<V> putAsync(K key, V value, long ttl, TimeUnit ttlUnit) {
        return putAsync(key, value, ttl, ttlUnit, 0, null);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
        return get(putAsync(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
    }

    @Override
    public RFuture<V> putAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
        checkKey(key);
        checkValue(value);

        if (ttl < 0) {
            throw new IllegalArgumentException("ttl can't be negative");
        }
        if (maxIdleTime < 0) {
            throw new IllegalArgumentException("maxIdleTime can't be negative");
        }
        if (ttl == 0 && maxIdleTime == 0) {
            return putAsync(key, value);
        }

        if (ttl > 0 && ttlUnit == null) {
            throw new NullPointerException("ttlUnit param can't be null");
        }

        if (maxIdleTime > 0 && maxIdleUnit == null) {
            throw new NullPointerException("maxIdleUnit param can't be null");
        }

        long ttlTimeout = 0;
        long ttlTimeoutDelta = 0;
        if (ttl > 0) {
            ttlTimeoutDelta = ttlUnit.toMillis(ttl);
            ttlTimeout = System.currentTimeMillis() + ttlTimeoutDelta;
        }

        long maxIdleTimeout = 0;
        long maxIdleDelta = 0;
        if (maxIdleTime > 0) {
            maxIdleDelta = maxIdleUnit.toMillis(maxIdleTime);
            maxIdleTimeout = System.currentTimeMillis() + maxIdleDelta;
        }

        RFuture<V> future = putOperationAsync(key, value, ttlTimeout, maxIdleTimeout, maxIdleDelta, ttlTimeoutDelta);
        if (hasNoWriter()) {
            return future;
        }

        MapWriterTask.Add listener = new MapWriterTask.Add(key, value);
        return mapWriterFuture(future, listener);
    }

    protected RFuture<V> putOperationAsync(K key, V value, long ttlTimeout, long maxIdleTimeout,
            long maxIdleDelta, long ttlTimeoutDelta) {
        String name = getName(key);
        RFuture<V> future = commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_MAP_VALUE,
                "local insertable = false; "
                        + "local v = redis.call('hget', KEYS[1], ARGV[5]); "
                        + "if v == false then "
                            + "insertable = true; "
                        + "else "
                            + "local t, val = struct.unpack('dLc0', v); "
                            + "local expireDate = 92233720368547758; "
                            + "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[5]); "
                            + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                            + "end; "
                            + "if t ~= 0 then "
                                + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[5]); "
                                + "if expireIdle ~= false then "
                                    + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                + "end; "
                            + "end; "
                            + "if expireDate <= tonumber(ARGV[1]) then "
                                + "insertable = true; "
                            + "end; "
                        + "end; "

                        + "if tonumber(ARGV[2]) > 0 then "
                            + "redis.call('zadd', KEYS[2], ARGV[2], ARGV[5]); "
                        + "else "
                            + "redis.call('zrem', KEYS[2], ARGV[5]); "
                        + "end; "
                        + "if tonumber(ARGV[3]) > 0 then "
                            + "redis.call('zadd', KEYS[3], ARGV[3], ARGV[5]); "
                        + "else "
                            + "redis.call('zrem', KEYS[3], ARGV[5]); "
                        + "end; "

                        // last access time
                        + "local maxSize = tonumber(redis.call('hget', KEYS[8], 'max-size')); " +
                        "if maxSize ~= nil and maxSize ~= 0 then " +
                        "    local currentTime = tonumber(ARGV[1]); " +
                        "    local lastAccessTimeSetName = KEYS[6]; " +

                    "        local mode = redis.call('hget', KEYS[8], 'mode'); " +
                    "        if mode == false or mode == 'LRU' then " +
                    "            redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[5]); " +
                    "        end; " +

                        "    local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                        "    if cacheSize >= maxSize then " +
                        "        local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize); " +
                        "        for index, lruItem in ipairs(lruItems) do " +
                        "            if lruItem and lruItem ~= ARGV[5] then " +
                        "                local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                        "                redis.call('hdel', KEYS[1], lruItem); " +
                        "                redis.call('zrem', KEYS[2], lruItem); " +
                        "                redis.call('zrem', KEYS[3], lruItem); " +
                        "                redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                                       " if lruItemValue ~= false then " +
                            "                local removedChannelName = KEYS[7]; " +
                                            "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                            "                redis.call('publish', removedChannelName, msg); " + 
                                        "end; " +
                        "            end; " +
                        "        end; " +
                        "    end; " +

                            "if mode == 'LFU' then " +
                                "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[5]); " +
                            "end; " +

                        "end; "

                        + "local value = struct.pack('dLc0', ARGV[4], string.len(ARGV[6]), ARGV[6]); "
                        + "redis.call('hset', KEYS[1], ARGV[5], value); "

                        + "if insertable == true then "
                            + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[5]), ARGV[5], string.len(ARGV[6]), ARGV[6]); "
                            + "redis.call('publish', KEYS[4], msg); "
                            + "return nil;"
                        + "end; "

                        + "local t, val = struct.unpack('dLc0', v); "

                        + "local msg = struct.pack('Lc0Lc0Lc0', string.len(ARGV[5]), ARGV[5], string.len(ARGV[6]), ARGV[6], string.len(val), val); "
                        + "redis.call('publish', KEYS[5], msg); "

                        + "return val",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getCreatedChannelName(name),
                        getUpdatedChannelName(name), getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), ttlTimeout, maxIdleTimeout, maxIdleDelta, encodeMapKey(key), encodeMapValue(value));
        return future;
    }
    
    @Override
    public long remainTimeToLive(K key) {
        return get(remainTimeToLiveAsync(key));
    }

    @Override
    public void setDefaultItemTTL(long ttl, TimeUnit ttlUnit) {
        Objects.requireNonNull(ttl);
        defaultTTL = ttlUnit.toMillis(ttl);
    }

    @Override
    public RFuture<Long> remainTimeToLiveAsync(K key) {
        checkKey(key);

        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_LONG,
                        "local value = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "if value == false then "
                            + "return -2; "
                        + "end; "
                        + "local t, val = struct.unpack('dLc0', value); "
                        + "local expireDate = 92233720368547758; " +
                        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); "
                        + "if expireDateScore ~= false then "
                            + "expireDate = tonumber(expireDateScore) "
                        + "end; "
                        + "if t ~= 0 then "
                            + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); "
                            + "if expireIdle ~= false then "
                                + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                            + "end; "
                        + "end; "
                            
                        + "if expireDate == 92233720368547758 then "
                            + "return -1; "
                        + "end;"
                        + "if expireDate > tonumber(ARGV[1]) then "
                            + "return expireDate - ARGV[1]; "
                        + "else "
                            + "return -2; "
                        + "end; "
                        + "return val; ",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name)),
                System.currentTimeMillis(), encodeMapKey(key));

    }

    String getTimeoutSetName(String name) {
        return prefixName("redisson__timeout__set", name);
    }

    String getTimeoutSetName() {
        return prefixName("redisson__timeout__set", getName());
    }
    
    String getLastAccessTimeSetName(String name) {
        return prefixName("redisson__map_cache__last_access__set", name);
    }

    String getLastAccessTimeSetName() {
        return prefixName("redisson__map_cache__last_access__set", getName());
    }

    String getIdleSetName(String name) {
        return prefixName("redisson__idle__set", name);
    }

    String getIdleSetName() {
        return prefixName("redisson__idle__set", getName());
    }

    String getOptionsName() {
        return suffixName(getName(), "redisson_options");
    }

    String getOptionsName(String name) {
        return suffixName(name, "redisson_options");
    }
    
    String getCreatedChannelName(String name) {
        return prefixName("redisson_map_cache_created", name);
    }

    String getCreatedChannelName() {
        return prefixName("redisson_map_cache_created", getName());
    }

    String getUpdatedChannelName() {
        return prefixName("redisson_map_cache_updated", getName());
    }
    
    String getUpdatedChannelName(String name) {
        return prefixName("redisson_map_cache_updated", name);
    }

    String getExpiredChannelName(String name) {
        return prefixName("redisson_map_cache_expired", name);
    }
    
    String getExpiredChannelName() {
        return prefixName("redisson_map_cache_expired", getName());
    }

    String getRemovedChannelName() {
        return prefixName("redisson_map_cache_removed", getName());
    }
    
    String getRemovedChannelName(String name) {
        return prefixName("redisson_map_cache_removed", name);
    }


    @Override
    protected RFuture<V> removeOperationAsync(K key) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_MAP_VALUE,
                "local value = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "if value == false then "
                            + "return nil; "
                        + "end; "

                        + "local t, val = struct.unpack('dLc0', value); "
                        + "local expireDate = 92233720368547758; " +
                        "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); "
                        + "if expireDateScore ~= false then "
                            + "expireDate = tonumber(expireDateScore) "
                        + "end; "
                        + "if t ~= 0 then "
                            + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); "
                            + "if expireIdle ~= false then "
                                + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                            + "end; "
                        + "end; "
                        + "if expireDate <= tonumber(ARGV[1]) then "
                            + "return nil; "
                        + "end; "

                        + "redis.call('zrem', KEYS[2], ARGV[2]); "
                        + "redis.call('zrem', KEYS[3], ARGV[2]); "
                        + "redis.call('zrem', KEYS[5], ARGV[2]); "
                        + "redis.call('hdel', KEYS[1], ARGV[2]); "

                        + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(val), val); "
                        + "redis.call('publish', KEYS[4], msg); "
                        + "return val; ",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getRemovedChannelName(name),
                        getLastAccessTimeSetName(name)),
                System.currentTimeMillis(), encodeMapKey(key));
    }

    @Override
    protected RFuture<List<Long>> fastRemoveOperationBatchAsync(K... keys) {
        List<Object> args = new ArrayList<Object>(keys.length);
        for (K key : keys) {
            args.add(encodeMapKey(key));
        }

        RFuture<List<Long>> future = commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_LIST,
                "local maxSize = tonumber(redis.call('hget', KEYS[6], 'max-size')); "
                        + "if maxSize ~= nil and maxSize ~= 0 then "
                        + "    redis.call('zrem', KEYS[5], unpack(ARGV)); "
                        + "end; " +
                        "redis.call('zrem', KEYS[3], unpack(ARGV)); " +
                        "redis.call('zrem', KEYS[2], unpack(ARGV)); " +
                        "for i, key in ipairs(ARGV) do "
                        + "local v = redis.call('hget', KEYS[1], key); "
                        + "if v ~= false then "
                            + "local t, val = struct.unpack('dLc0', v); "
                            + "local msg = struct.pack('Lc0Lc0', string.len(key), key, string.len(val), val); "
                            + "redis.call('publish', KEYS[4], msg); "
                        + "end; " +
                        "end; " +

                        "local result = {}; " +
                        "for i = 1, #ARGV, 1 do "
                            + "local val = redis.call('hdel', KEYS[1], ARGV[i]); "
                            + "table.insert(result, val); "
                        + "end;"
                        + "return result;",
                Arrays.asList(getName(), getTimeoutSetName(), getIdleSetName(), getRemovedChannelName(), getLastAccessTimeSetName(), getOptionsName()),
                args.toArray());
        return future;
    }

    @Override
    protected RFuture<Long> fastRemoveOperationAsync(K... keys) {
        List<Object> params = new ArrayList<Object>(keys.length);
        for (K key : keys) {
            params.add(encodeMapKey(key));
        }

        return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_LONG,
                "local maxSize = tonumber(redis.call('hget', KEYS[6], 'max-size')); "
                        + "if maxSize ~= nil and maxSize ~= 0 then "
                        + "    redis.call('zrem', KEYS[5], unpack(ARGV)); "
                        + "end; " +
                        "redis.call('zrem', KEYS[3], unpack(ARGV)); " +
                        "redis.call('zrem', KEYS[2], unpack(ARGV)); " +
                        "for i, key in ipairs(ARGV) do "
                        + "local v = redis.call('hget', KEYS[1], key); "
                        + "if v ~= false then "
                            + "local t, val = struct.unpack('dLc0', v); "
                            + "local msg = struct.pack('Lc0Lc0', string.len(key), key, string.len(val), val); "
                            + "redis.call('publish', KEYS[4], msg); "
                        + "end; " +
                        "end; " +
                        "return redis.call('hdel', KEYS[1], unpack(ARGV)); ",
                Arrays.asList(getName(), getTimeoutSetName(), getIdleSetName(), getRemovedChannelName(), getLastAccessTimeSetName(), getOptionsName()),
                params.toArray());
    }

    @Override
    public MapScanResult<Object, Object> scanIterator(String name, RedisClient client, long startPos, String pattern, int count) {
        return get(scanIteratorAsync(name, client, startPos, pattern, count));
    }

    private static final RedisCommand<MapCacheScanResult<Object, Object>> SCAN = new RedisCommand<MapCacheScanResult<Object, Object>>("EVAL",
                new ListMultiDecoder2(
                        new MapCacheScanResultReplayDecoder(),
                        new ObjectMapDecoder(true)));

    @Override
    public RFuture<MapScanResult<Object, Object>> scanIteratorAsync(String name, RedisClient client, long startPos, String pattern, int count) {
        List<Object> params = new ArrayList<Object>();
        params.add(System.currentTimeMillis());
        params.add(startPos);
        if (pattern != null) {
            params.add(pattern);
        }
        params.add(count);

        RFuture<MapCacheScanResult<Object, Object>> f = commandExecutor.evalReadAsync(client, name, codec, SCAN,
                "local result = {}; "
                + "local idleKeys = {}; "
                + "local res; "
                + "if (#ARGV == 4) then "
                    + " res = redis.call('hscan', KEYS[1], ARGV[2], 'match', ARGV[3], 'count', ARGV[4]); "
                + "else "
                    + " res = redis.call('hscan', KEYS[1], ARGV[2], 'count', ARGV[3]); "
                + "end;"
                + "local currentTime = tonumber(ARGV[1]); "
                + "for i, value in ipairs(res[2]) do "
                    + "if i % 2 == 0 then "
                      + "local key = res[2][i-1]; " +
                        "local expireDate = 92233720368547758; " +
                        "local expireDateScore = redis.call('zscore', KEYS[2], key); "
                        + "if expireDateScore ~= false then "
                            + "expireDate = tonumber(expireDateScore) "
                        + "end; "

                        + "local t, val = struct.unpack('dLc0', value); "
                        + "if t ~= 0 then "
                            + "local expireIdle = redis.call('zscore', KEYS[3], key); "
                            + "if expireIdle ~= false then "
                                + "if tonumber(expireIdle) > currentTime and expireDate > currentTime then "
                                    + "table.insert(idleKeys, key); "
                                + "end; "
                                + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                            + "end; "
                        + "end; "

                        + "if expireDate > currentTime then "
                            + "table.insert(result, key); "
                            + "table.insert(result, val); "
                        + "end; "
                    + "end; "
                + "end;"
                + "return {res[1], result, idleKeys};",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name)),
                params.toArray());

        f.onComplete((res, e) -> {
            if (res != null) {
                if (res.getIdleKeys().isEmpty()) {
                    return;
                }

                List<Object> args = new ArrayList<Object>(res.getIdleKeys().size() + 1);
                args.add(System.currentTimeMillis());
                encodeMapKeys(args, res.getIdleKeys());

                commandExecutor.evalWriteAsync(name, codec, new RedisCommand<Map<Object, Object>>("EVAL",
                                new MapValueDecoder(new MapGetAllDecoder(args, 1))),
                                "local currentTime = tonumber(table.remove(ARGV, 1)); " // index is the first parameter
                              + "local map = redis.call('hmget', KEYS[1], unpack(ARGV)); "
                              + "for i = #map, 1, -1 do "
                                  + "local value = map[i]; "
                                  + "if value ~= false then "
                                      + "local key = ARGV[i]; "
                                      + "local t, val = struct.unpack('dLc0', value); "

                                      + "if t ~= 0 then "
                                          + "local expireIdle = redis.call('zscore', KEYS[2], key); "
                                          + "if expireIdle ~= false then "
                                              + "if tonumber(expireIdle) > currentTime then "
                                                  + "redis.call('zadd', KEYS[2], t + currentTime, key); "
                                              + "end; "
                                          + "end; "
                                      + "end; "
                                  + "end; "
                              + "end; ",
                        Arrays.asList(name, getIdleSetName(name)), args.toArray());
            }
        });

        return (RFuture<MapScanResult<Object, Object>>) (Object) f;
    }

    @Override
    protected RFuture<Boolean> fastPutOperationAsync(K key, V value) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_BOOLEAN,
                "local insertable = false; "
                        + "local v = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "if v == false then "
                                + "insertable = true; "
                        + "else "
                            + "local t, val = struct.unpack('dLc0', v); "
                            + "local expireDate = 92233720368547758; "
                            + "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); "
                            + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                            + "end; "
                            + "if t ~= 0 then "
                                + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); "
                                + "if expireIdle ~= false then "
                                    + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                + "end; "
                            + "end; "
                            + "if expireDate <= tonumber(ARGV[1]) then "
                                + "insertable = true; "
                            + "end; "
                        + "end; " +

                        "local val = struct.pack('dLc0', 0, string.len(ARGV[3]), ARGV[3]); "
                        + "redis.call('hset', KEYS[1], ARGV[2], val); " +

                        // last access time
                        "local maxSize = tonumber(redis.call('hget', KEYS[8], 'max-size'));" +
                        "if maxSize ~= nil and maxSize ~= 0 then " +
                        "    local currentTime = tonumber(ARGV[1]); " +
                        "    local lastAccessTimeSetName = KEYS[6]; " +
                            "local mode = redis.call('hget', KEYS[8], 'mode'); " +
                            "if mode == false or mode == 'LRU' then " +
                                "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[2]); " +
                            "end; " +
                        "    local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                        "    if cacheSize > maxSize then " +
                        "        local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize - 1); " +
                        "        for index, lruItem in ipairs(lruItems) do " +
                        "            if lruItem and lruItem ~= ARGV[2] then " +
                        "                local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                        "                redis.call('hdel', KEYS[1], lruItem); " +
                        "                redis.call('zrem', KEYS[2], lruItem); " +
                        "                redis.call('zrem', KEYS[3], lruItem); " +
                        "                redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                                       " if lruItemValue ~= false then " +
                            "                local removedChannelName = KEYS[7]; " +
                                            "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                            "                redis.call('publish', removedChannelName, msg); " + 
                                        "end; " +
                        "            end; " +
                        "        end; " +
                        "    end; " +

                            "if mode == 'LFU' then " +
                                "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[2]); " +
                            "end; " +

                        "end; "

                        + "if insertable == true then "
                            + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3]); "
                            + "redis.call('publish', KEYS[4], msg); "
                            + "return 1;"
                        + "else "
                            + "local t, val = struct.unpack('dLc0', v); "
                            + "local msg = struct.pack('Lc0Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3], string.len(val), val); "
                            + "redis.call('publish', KEYS[5], msg); "
                            + "return 0;"
                        + "end;",
                Arrays.asList(getName(key), getTimeoutSetName(name), getIdleSetName(name), getCreatedChannelName(name),
                        getUpdatedChannelName(name), getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), encodeMapKey(key), encodeMapValue(value));
    }

    @Override
    protected RFuture<Boolean> fastPutIfExistsOperationAsync(K key, V value) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_BOOLEAN,
                "local value = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "local lastAccessTimeSetName = KEYS[5]; "
                        + "local maxSize = tonumber(redis.call('hget', KEYS[7], 'max-size')); "
                        + "local currentTime = tonumber(ARGV[1]); "
                        + "if value ~= false then "
                            + "local val = struct.pack('dLc0', 0, string.len(ARGV[3]), ARGV[3]); "
                            + "redis.call('hset', KEYS[1], ARGV[2], val); "
                            + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3]); "
                            + "redis.call('publish', KEYS[4], msg); "+

                            // last access time

                            "if maxSize ~= nil and maxSize ~= 0 then " +
                                "local mode = redis.call('hget', KEYS[7], 'mode'); " +
                                "if mode == false or mode == 'LRU' then " +
                                    "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[2]); " +
                                "end; " +
                            "    local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                            "    if cacheSize > maxSize then " +
                            "        local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize - 1); " +
                            "        for index, lruItem in ipairs(lruItems) do " +
                            "            if lruItem and lruItem ~= ARGV[2] then " +
                            "                local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                            "                redis.call('hdel', KEYS[1], lruItem); " +
                            "                redis.call('zrem', KEYS[2], lruItem); " +
                            "                redis.call('zrem', KEYS[3], lruItem); " +
                            "                redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                                           " if lruItemValue ~= false then " +
                                "                local removedChannelName = KEYS[6]; " +
                                                "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                                "                local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                                "                redis.call('publish', removedChannelName, msg); " +
                                            "end; " +
                            "            end; " +
                            "        end; " +
                            "    end; " +

                                "if mode == 'LFU' then " +
                                    "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[2]); " +
                                "end; " +

                            "end; "

                            + "return 1; "
                        + "end; "

                        + "return 0; ",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getCreatedChannelName(name),
                        getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), encodeMapKey(key), encodeMapValue(value));
    }

    @Override
    protected RFuture<Boolean> fastPutIfAbsentOperationAsync(K key, V value) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_BOOLEAN,
                "local value = redis.call('hget', KEYS[1], ARGV[2]); "
                        + "local lastAccessTimeSetName = KEYS[5]; "
                        + "local maxSize = tonumber(redis.call('hget', KEYS[7], 'max-size')); "
                        + "local currentTime = tonumber(ARGV[1]); "
                        + "if value == false then "
                            + "local val = struct.pack('dLc0', 0, string.len(ARGV[3]), ARGV[3]); "
                            + "redis.call('hset', KEYS[1], ARGV[2], val); "
                            + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3]); "
                            + "redis.call('publish', KEYS[4], msg); "+

                            // last access time

                            "if maxSize ~= nil and maxSize ~= 0 then " +
                                "local mode = redis.call('hget', KEYS[7], 'mode'); " +
                                "if mode == false or mode == 'LRU' then " +
                                    "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[2]); " +
                                "end; " +
                            "    local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                            "    if cacheSize > maxSize then " +
                            "        local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize - 1); " +
                            "        for index, lruItem in ipairs(lruItems) do " +
                            "            if lruItem and lruItem ~= ARGV[2] then " +
                            "                local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                            "                redis.call('hdel', KEYS[1], lruItem); " +
                            "                redis.call('zrem', KEYS[2], lruItem); " +
                            "                redis.call('zrem', KEYS[3], lruItem); " +
                            "                redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                                           " if lruItemValue ~= false then " +
                                "                local removedChannelName = KEYS[6]; " +
                                                "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                                "                local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                                "                redis.call('publish', removedChannelName, msg); " + 
                                            "end; " +
                            "            end; " +
                            "        end; " +
                            "    end; " +

                                "if mode == 'LFU' then " +
                                    "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[2]); " +
                                "end; " +

                            "end; "

                            + "return 1; "
                        + "end; "

                        + "if maxSize ~= nil and maxSize ~= 0 then " +
                                "local mode = redis.call('hget', KEYS[7], 'mode'); " +
                                "if mode == false or mode == 'LRU' then " +
                                    "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[2]); " +
                                "else " +
                                    "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[2]); " +
                                "end; "
                        + "end; "
                        + "local t, val = struct.unpack('dLc0', value); "
                        + "local expireDate = 92233720368547758; "
                        + "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); "
                        + "if expireDateScore ~= false then "
                            + "expireDate = tonumber(expireDateScore) "
                        + "end; "
                        + "if t ~= 0 then "
                            + "local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); "
                            + "if expireIdle ~= false then "
                                + "if tonumber(expireIdle) > tonumber(ARGV[1]) then "
                                    + "redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), ARGV[2]); "
                                + "end; "
                                + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                            + "end; "
                        + "end; "
                        + "if expireDate > tonumber(ARGV[1]) then "
                            + "return 0; "
                        + "end; "

                        + "redis.call('zrem', KEYS[2], ARGV[2]); "
                        + "redis.call('zrem', KEYS[3], ARGV[2]); "
                        + "local val = struct.pack('dLc0', 0, string.len(ARGV[3]), ARGV[3]); "
                        + "redis.call('hset', KEYS[1], ARGV[2], val); "

                        + "local msg = struct.pack('Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3]); "
                        + "redis.call('publish', KEYS[4], msg); "
                        + "return 1; ",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getCreatedChannelName(name),
                        getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), encodeMapKey(key), encodeMapValue(value));
    }

    @Override
    public boolean fastPutIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit) {
        return fastPutIfAbsent(key, value, ttl, ttlUnit, 0, null);
    }

    @Override
    public boolean fastPutIfAbsent(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
        return get(fastPutIfAbsentAsync(key, value, ttl, ttlUnit, maxIdleTime, maxIdleUnit));
    }

    @Override
    public RFuture<Boolean> fastPutIfAbsentAsync(K key, V value, long ttl, TimeUnit ttlUnit, long maxIdleTime, TimeUnit maxIdleUnit) {
        checkKey(key);
        checkValue(value);

        if (ttl < 0) {
            throw new IllegalArgumentException("ttl can't be negative");
        }
        if (maxIdleTime < 0) {
            throw new IllegalArgumentException("maxIdleTime can't be negative");
        }
        if (ttl == 0 && maxIdleTime == 0) {
            return fastPutIfAbsentAsync(key, value);
        }

        if (ttl > 0 && ttlUnit == null) {
            throw new NullPointerException("ttlUnit param can't be null");
        }

        if (maxIdleTime > 0 && maxIdleUnit == null) {
            throw new NullPointerException("maxIdleUnit param can't be null");
        }

        long ttlTimeout = 0;
        if (ttl > 0) {
            ttlTimeout = System.currentTimeMillis() + ttlUnit.toMillis(ttl);
        }

        long maxIdleTimeout = 0;
        long maxIdleDelta = 0;
        if (maxIdleTime > 0) {
            maxIdleDelta = maxIdleUnit.toMillis(maxIdleTime);
            maxIdleTimeout = System.currentTimeMillis() + maxIdleDelta;
        }

        String name = getName(key);
        RFuture<Boolean> future = commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_BOOLEAN,
                "local insertable = false; " +
                        "local value = redis.call('hget', KEYS[1], ARGV[5]); " +
                        "if value == false then " +
                        "    insertable = true; " +
                        "else " +
                        "    if insertable == false then " +
                        "        local t, val = struct.unpack('dLc0', value); " +
                        "        local expireDate = 92233720368547758; " +
                        "        local expireDateScore = redis.call('zscore', KEYS[2], ARGV[5]); " +
                        "        if expireDateScore ~= false then " +
                        "            expireDate = tonumber(expireDateScore) " +
                        "        end; " +
                        "        if t ~= 0 then " +
                        "            local expireIdle = redis.call('zscore', KEYS[3], ARGV[5]); " +
                        "            if expireIdle ~= false then " +
                        "                expireDate = math.min(expireDate, tonumber(expireIdle)) " +
                        "            end; " +
                        "        end; " +
                        "        if expireDate <= tonumber(ARGV[1]) then " +
                        "            insertable = true; " +
                        "        end; " +
                        "    end; " +
                        "end; " +
                        "if insertable == true then " +
                             // ttl
                        "    if tonumber(ARGV[2]) > 0 then " +
                        "        redis.call('zadd', KEYS[2], ARGV[2], ARGV[5]); " +
                        "    else " +
                        "        redis.call('zrem', KEYS[2], ARGV[5]); " +
                        "    end; " +
                             // idle
                        "    if tonumber(ARGV[3]) > 0 then " +
                        "        redis.call('zadd', KEYS[3], ARGV[3], ARGV[5]); " +
                        "    else " +
                        "        redis.call('zrem', KEYS[3], ARGV[5]); " +
                        "    end; " +
                             // last access time
                        "    local maxSize = tonumber(redis.call('hget', KEYS[7], 'max-size')); " +
                        "    if maxSize ~= nil and maxSize ~= 0 then " +
                        "        local currentTime = tonumber(ARGV[1]); " +
                        "        local lastAccessTimeSetName = KEYS[5]; " +

                                "local mode = redis.call('hget', KEYS[7], 'mode'); " +
                                "if mode == false or mode == 'LRU' then " +
                                    "redis.call('zadd', lastAccessTimeSetName, currentTime, ARGV[5]); " +
                                "end; " +

                        "        local cacheSize = tonumber(redis.call('hlen', KEYS[1])); " +
                        "        if cacheSize >= maxSize then " +
                        "            local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize); " +
                        "            for index, lruItem in ipairs(lruItems) do " +
                        "                if lruItem and lruItem ~= ARGV[5] then " +
                        "                    local lruItemValue = redis.call('hget', KEYS[1], lruItem); " +
                        "                    redis.call('hdel', KEYS[1], lruItem); " +
                        "                    redis.call('zrem', KEYS[2], lruItem); " +
                        "                    redis.call('zrem', KEYS[3], lruItem); " +
                        "                    redis.call('zrem', lastAccessTimeSetName, lruItem); " +
                                           " if lruItemValue ~= false then " +
                            "                    local removedChannelName = KEYS[6]; " +
                                                "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                    local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                            "                    redis.call('publish', removedChannelName, msg); " + 
                                            "end; " +
                        "                end; " +
                        "            end; " +
                        "        end; " +

                                "if mode == 'LFU' then " +
                                    "redis.call('zincrby', lastAccessTimeSetName, 1, ARGV[5]); " +
                                "end; " +

                        "    end; " +
                             // value
                        "    local val = struct.pack('dLc0', ARGV[4], string.len(ARGV[6]), ARGV[6]); " +
                        "    redis.call('hset', KEYS[1], ARGV[5], val); " +
                        "    local msg = struct.pack('Lc0Lc0', string.len(ARGV[5]), ARGV[5], string.len(ARGV[6]), ARGV[6]); " +
                        "    redis.call('publish', KEYS[4], msg); " +
                        "    return 1; " +
                        "else " +
                        "    return 0; " +
                        "end; ",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getCreatedChannelName(name),
                        getLastAccessTimeSetName(name), getRemovedChannelName(name), getOptionsName(name)),
                System.currentTimeMillis(), ttlTimeout, maxIdleTimeout, maxIdleDelta, encodeMapKey(key), encodeMapValue(value));
        if (hasNoWriter()) {
            return future;
        }

        MapWriterTask.Add listener = new MapWriterTask.Add(key, value);
        return mapWriterFuture(future, listener, Function.identity());
    }

    @Override
    protected RFuture<Boolean> replaceOperationAsync(K key, V oldValue, V newValue) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_BOOLEAN,
            "local v = redis.call('hget', KEYS[1], ARGV[2]); " +
            "if v == false then " +
            "    return 0; " +
            "end; " +
            "local expireDate = 92233720368547758; " +
            "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); " +
            "if expireDateScore ~= false then " +
            "    expireDate = tonumber(expireDateScore) " +
            "end; " +
            "" +
            "local t, val = struct.unpack('dLc0', v); " +
            "if t ~= 0 then " +
            "    local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); " +
            "    if tonumber(expireIdle) > tonumber(ARGV[1]) then " +
            "        redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), ARGV[2]); " +
            "    end ;" +
            "    if expireIdle ~= false then " +
            "        expireDate = math.min(expireDate, tonumber(expireIdle)) " +
            "    end; " +
            "end; " +
            "if expireDate > tonumber(ARGV[1]) and val == ARGV[3] then " +
            "    local msg = struct.pack('Lc0Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[4]), ARGV[4], string.len(ARGV[3]), ARGV[3]); " +
            "    redis.call('publish', KEYS[4], msg); " +
            "" +
            "    local value = struct.pack('dLc0', t, string.len(ARGV[4]), ARGV[4]); " +
            "    redis.call('hset', KEYS[1], ARGV[2], value); " +
            "    return 1; " +
            "end; " +
            "return 0; ",
            Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getUpdatedChannelName(name)),
            System.currentTimeMillis(), encodeMapKey(key), encodeMapValue(oldValue), encodeMapValue(newValue));
    }

    @Override
    protected RFuture<Boolean> fastReplaceOperationAsync(K key, V value) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_BOOLEAN,
                "local value = redis.call('hget', KEYS[1], ARGV[2]); " +
                "if value == false then " +
                "    return 0; " +
                "end; " +
                "local t, val = struct.unpack('dLc0', value); " +
                "local expireDate = 92233720368547758; " +
                "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); " +
                "if expireDateScore ~= false then " +
                "    expireDate = tonumber(expireDateScore) " +
                "end; " +
                "if t ~= 0 then " +
                "    local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); " +
                "    if tonumber(expireIdle) > tonumber(ARGV[1]) then " +
                "        redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), ARGV[2]); " +
                "    end ;" +
                "    if expireIdle ~= false then " +
                "        expireDate = math.min(expireDate, tonumber(expireIdle)) " +
                "    end; " +
                "end; " +
                "if expireDate <= tonumber(ARGV[1]) then " +
                "    return 0; " +
                "end; " +
                "local value = struct.pack('dLc0', t, string.len(ARGV[3]), ARGV[3]); " +
                "redis.call('hset', KEYS[1], ARGV[2], value); " +
                "local msg = struct.pack('Lc0Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3], string.len(val), val); " +
                "redis.call('publish', KEYS[4], msg); " +
                "return 1; ",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getUpdatedChannelName(name)),
                System.currentTimeMillis(), encodeMapKey(key), encodeMapValue(value));
    }
    
    @Override
    protected RFuture<V> replaceOperationAsync(K key, V value) {
        String name = getName(key);
        return commandExecutor.evalWriteAsync(name, codec, RedisCommands.EVAL_MAP_VALUE,
                "local value = redis.call('hget', KEYS[1], ARGV[2]); " +
                "if value == false then " +
                "    return nil; " +
                "end; " +
                "local t, val = struct.unpack('dLc0', value); " +
                "local expireDate = 92233720368547758; " +
                "local expireDateScore = redis.call('zscore', KEYS[2], ARGV[2]); " +
                "if expireDateScore ~= false then " +
                "    expireDate = tonumber(expireDateScore) " +
                "end; " +
                "if t ~= 0 then " +
                "    local expireIdle = redis.call('zscore', KEYS[3], ARGV[2]); " +
                "    if tonumber(expireIdle) > tonumber(ARGV[1]) then " +
                "        redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), ARGV[2]); " +
                "    end ;" +
                "    if expireIdle ~= false then " +
                "        expireDate = math.min(expireDate, tonumber(expireIdle)) " +
                "    end; " +
                "end; " +
                "if expireDate <= tonumber(ARGV[1]) then " +
                "    return nil; " +
                "end; " +
                "local value = struct.pack('dLc0', t, string.len(ARGV[3]), ARGV[3]); " +
                "redis.call('hset', KEYS[1], ARGV[2], value); " +
                "local msg = struct.pack('Lc0Lc0Lc0', string.len(ARGV[2]), ARGV[2], string.len(ARGV[3]), ARGV[3], string.len(val), val); " +
                "redis.call('publish', KEYS[4], msg); " +
                "return val; ",
                Arrays.asList(name, getTimeoutSetName(name), getIdleSetName(name), getUpdatedChannelName(name)),
                System.currentTimeMillis(), encodeMapKey(key), encodeMapValue(value));
    }

    @Override
    protected RFuture<Void> putAllOperationAsync(Map<? extends K, ? extends V> map) {
        List<Object> params = new ArrayList<Object>(map.size()*2 + 1);
        params.add(System.currentTimeMillis());
        for (java.util.Map.Entry<? extends K, ? extends V> t : map.entrySet()) {
            if (t.getKey() == null) {
                throw new NullPointerException("map key can't be null");
            }
            if (t.getValue() == null) {
                throw new NullPointerException("map value can't be null");
            }

            params.add(encodeMapKey(t.getKey()));
            params.add(encodeMapValue(t.getValue()));
        }

        return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_VOID,
                  "local currentTime = tonumber(table.remove(ARGV, 1)); " + // index is the first parameter
                  "local maxSize = tonumber(redis.call('hget', KEYS[8], 'max-size'));" +
                  "local mode = redis.call('hget', KEYS[8], 'mode'); " +
                  "for i, value in ipairs(ARGV) do "
                    + "if i % 2 == 0 then " 
                      + "local key = ARGV[i-1];" +

                        "local v = redis.call('hget', KEYS[1], key);" +
                        "local exists = false;" +
                        "if v ~= false then" +
                        "    local t, val = struct.unpack('dLc0', v);" +
                        "    local expireDate = 92233720368547758;" +
                        "    local expireDateScore = redis.call('zscore', KEYS[2], key);" +
                        "    if expireDateScore ~= false then" +
                        "        expireDate = tonumber(expireDateScore)" +
                        "    end;" +
                        "    if t ~= 0 then" +
                        "        local expireIdle = redis.call('zscore', KEYS[3], key);" +
                        "        if expireIdle ~= false then" +
                        "            expireDate = math.min(expireDate, tonumber(expireIdle))" +
                        "        end;" +
                        "    end;" +
                        "    if expireDate > tonumber(currentTime) then" +
                        "        exists = true;" +
                        "    end;" +
                        "end;" +
                        "" +
                        "local newvalue = struct.pack('dLc0', 0, string.len(value), value);" +
                        "redis.call('hset', KEYS[1], key, newvalue);" +

                        "local lastAccessTimeSetName = KEYS[6];" +
                        "if exists == false then" +
                        "    if maxSize ~= nil and maxSize ~= 0 then " +
                                "if mode == false or mode == 'LRU' then " +
                                    "redis.call('zadd', lastAccessTimeSetName, currentTime, key); " +
                                "end; " +

                        "        local cacheSize = tonumber(redis.call('hlen', KEYS[1]));" +
                        "        if cacheSize > maxSize then" +
                        "            local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize - 1);" +
                        "            for index, lruItem in ipairs(lruItems) do" +
                        "                if lruItem and lruItem ~= key then" +
                        "                    local lruItemValue = redis.call('hget', KEYS[1], lruItem);" +
                        "                    redis.call('hdel', KEYS[1], lruItem);" +
                        "                    redis.call('zrem', KEYS[2], lruItem);" +
                        "                    redis.call('zrem', KEYS[3], lruItem);" +
                        "                    redis.call('zrem', lastAccessTimeSetName, lruItem);" +
                                           " if lruItemValue ~= false then " +
                            "                    local removedChannelName = KEYS[7];" +
                                                "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                    local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                            "                    redis.call('publish', removedChannelName, msg);"
                                          + "end; " +
                        "                end;" +
                        "            end" +
                        "        end;" +

                                "if mode == 'LFU' then " +
                                    "redis.call('zincrby', lastAccessTimeSetName, 1, key); " +
                                "end; " +

                        "    end;" +
                        "    local msg = struct.pack('Lc0Lc0', string.len(key), key, string.len(value), value);" +
                        "    redis.call('publish', KEYS[4], msg);" +
                        "else " +
                            "local t, val = struct.unpack('dLc0', v);" +
                            "local msg = struct.pack('Lc0Lc0Lc0', string.len(key), key, string.len(value), value, string.len(val), val);" +
                            "redis.call('publish', KEYS[5], msg);" + 
                            
                        "    if maxSize ~= nil and maxSize ~= 0 then " +
                                "if mode == false or mode == 'LRU' then " +
                                    "redis.call('zadd', lastAccessTimeSetName, currentTime, key); " +
                                "else " +
                                    "redis.call('zincrby', lastAccessTimeSetName, 1, key); " +
                                "end; " +
                        "    end;" +
                        "end;"
                    + "end;"
                + "end;",
                Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getCreatedChannelName(),
                        getUpdatedChannelName(), getLastAccessTimeSetName(), getRemovedChannelName(), getOptionsName()),
            params.toArray());
    }
    
    private RFuture<Void> putAllOperationAsync(Map<? extends K, ? extends V> map, long ttl, TimeUnit ttlUnit) {
        List<Object> params = new ArrayList<Object>(map.size()*2 + 2);
        params.add(System.currentTimeMillis());
        long ttlTimeout = 0;
        if (ttl > 0) {
            ttlTimeout = System.currentTimeMillis() + ttlUnit.toMillis(ttl);
        }
        params.add(ttlTimeout);
        for (java.util.Map.Entry<? extends K, ? extends V> t : map.entrySet()) {
            if (t.getKey() == null) {
                throw new NullPointerException("map key can't be null");
            }
            if (t.getValue() == null) {
                throw new NullPointerException("map value can't be null");
            }

            params.add(encodeMapKey(t.getKey()));
            params.add(encodeMapValue(t.getValue()));
        }

        return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_VOID,
                  "local currentTime = tonumber(table.remove(ARGV, 1)); " + // index is the first parameter
                  "local ttl = table.remove(ARGV, 1); " + // ttl is the second parameter
                  "local ttlNumber = tonumber(ttl); " +
                  "local maxSize = tonumber(redis.call('hget', KEYS[8], 'max-size'));" +
                  "local mode = redis.call('hget', KEYS[8], 'mode'); " +
                  "for i, value in ipairs(ARGV) do "
                    + "if i % 2 == 0 then " 
                      + "local key = ARGV[i-1];" +

                        "local v = redis.call('hget', KEYS[1], key);" +
                        "local exists = false;" +
                        "if v ~= false then" +
                        "    local t, val = struct.unpack('dLc0', v);" +
                        "    local expireDate = 92233720368547758;" +
                        "    local expireDateScore = redis.call('zscore', KEYS[2], key);" +
                        "    if expireDateScore ~= false then" +
                        "        expireDate = tonumber(expireDateScore)" +
                        "    end;" +
                        "    if t ~= 0 then" +
                        "        local expireIdle = redis.call('zscore', KEYS[3], key);" +
                        "        if expireIdle ~= false then" +
                        "            expireDate = math.min(expireDate, tonumber(expireIdle))" +
                        "        end;" +
                        "    end;" +
                        "    if expireDate > tonumber(currentTime) then" +
                        "        exists = true;" +
                        "    end;" +
                        "end;" +
                        "" +
                        "if ttlNumber > 0 then " +
                        "    redis.call('zadd', KEYS[2], ttl, key); " +
                        "else " +
                        "    redis.call('zrem', KEYS[2], key); " +
                        "end; " +
                        "" +
                        "local newvalue = struct.pack('dLc0', 0, string.len(value), value);" +
                        "redis.call('hset', KEYS[1], key, newvalue);" +

                        "local lastAccessTimeSetName = KEYS[6];" +
                        "if exists == false then" +
                        "    if maxSize ~= nil and maxSize ~= 0 then " +
                                "if mode == false or mode == 'LRU' then " +
                                    "redis.call('zadd', lastAccessTimeSetName, currentTime, key); " +
                                "end; " +

                        "        local cacheSize = tonumber(redis.call('hlen', KEYS[1]));" +
                        "        if cacheSize > maxSize then" +
                        "            local lruItems = redis.call('zrange', lastAccessTimeSetName, 0, cacheSize - maxSize - 1);" +
                        "            for index, lruItem in ipairs(lruItems) do" +
                        "                if lruItem and lruItem ~= key then" +
                        "                    local lruItemValue = redis.call('hget', KEYS[1], lruItem);" +
                        "                    redis.call('hdel', KEYS[1], lruItem);" +
                        "                    redis.call('zrem', KEYS[2], lruItem);" +
                        "                    redis.call('zrem', KEYS[3], lruItem);" +
                        "                    redis.call('zrem', lastAccessTimeSetName, lruItem);" +
                                           " if lruItemValue ~= false then " +
                            "                    local removedChannelName = KEYS[7];" +
                                                "local ttl, obj = struct.unpack('dLc0', lruItemValue);" +
                            "                    local msg = struct.pack('Lc0Lc0', string.len(lruItem), lruItem, string.len(obj), obj);" +
                            "                    redis.call('publish', removedChannelName, msg);"
                                          + "end; " +
                        "                end;" +
                        "            end" +
                        "        end;" +

                                "if mode == 'LFU' then " +
                                    "redis.call('zincrby', lastAccessTimeSetName, 1, key); " +
                                "end; " +

                        "    end;" +
                        "    local msg = struct.pack('Lc0Lc0', string.len(key), key, string.len(value), value);" +
                        "    redis.call('publish', KEYS[4], msg);" +
                        "else " +
                            "local t, val = struct.unpack('dLc0', v);" +
                            "local msg = struct.pack('Lc0Lc0Lc0', string.len(key), key, string.len(value), value, string.len(val), val);" +
                            "redis.call('publish', KEYS[5], msg);" + 
                            
                        "    if maxSize ~= nil and maxSize ~= 0 then " +
                                "if mode == false or mode == 'LRU' then " +
                                    "redis.call('zadd', lastAccessTimeSetName, currentTime, key); " +
                                "else " +
                                    "redis.call('zincrby', lastAccessTimeSetName, 1, key); " +
                                "end; " +
                        "    end;" +
                        "end;"
                    + "end;"
                + "end;",
                Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getCreatedChannelName(),
                        getUpdatedChannelName(), getLastAccessTimeSetName(), getRemovedChannelName(), getOptionsName()),
            params.toArray());
    }

    private MapCacheEventCodec.OSType osType;
    
    @Override
    public int addListener(MapEntryListener listener) {
        if (listener == null) {
            throw new NullPointerException();
        }
        
        if (osType == null) {
            RFuture<Map<String, String>> serverFuture = commandExecutor.readAsync((String) null, StringCodec.INSTANCE, RedisCommands.INFO_SERVER);
            serverFuture.syncUninterruptibly();
            String os = serverFuture.getNow().get("os");
            if (os.contains("Windows")) {
                osType = BaseEventCodec.OSType.WINDOWS;
            } else if (os.contains("NONSTOP")) {
                osType = BaseEventCodec.OSType.HPNONSTOP;
            }
        }

        if (listener instanceof EntryRemovedListener) {
            RTopic topic = redisson.getTopic(getRemovedChannelName(), new MapCacheEventCodec(codec, osType));
            return topic.addListener(List.class, new MessageListener<List<Object>>() {
                @Override
                public void onMessage(CharSequence channel, List<Object> msg) {
                    EntryEvent<K, V> event = new EntryEvent<K, V>(RedissonMapCache.this, EntryEvent.Type.REMOVED, (K) msg.get(0), (V) msg.get(1), null);
                    ((EntryRemovedListener<K, V>) listener).onRemoved(event);
                }
            });
        }

        if (listener instanceof EntryCreatedListener) {
            RTopic topic = redisson.getTopic(getCreatedChannelName(), new MapCacheEventCodec(codec, osType));
            return topic.addListener(List.class, new MessageListener<List<Object>>() {
                @Override
                public void onMessage(CharSequence channel, List<Object> msg) {
                    EntryEvent<K, V> event = new EntryEvent<K, V>(RedissonMapCache.this, EntryEvent.Type.CREATED, (K) msg.get(0), (V) msg.get(1), null);
                    ((EntryCreatedListener<K, V>) listener).onCreated(event);
                }
            });
        }

        if (listener instanceof EntryUpdatedListener) {
            RTopic topic = redisson.getTopic(getUpdatedChannelName(), new MapCacheEventCodec(codec, osType));
            return topic.addListener(List.class, new MessageListener<List<Object>>() {
                @Override
                public void onMessage(CharSequence channel, List<Object> msg) {
                    EntryEvent<K, V> event = new EntryEvent<K, V>(RedissonMapCache.this, EntryEvent.Type.UPDATED, (K) msg.get(0), (V) msg.get(1), (V) msg.get(2));
                    ((EntryUpdatedListener<K, V>) listener).onUpdated(event);
                }
            });
        }

        if (listener instanceof EntryExpiredListener) {
            RTopic topic = redisson.getTopic(getExpiredChannelName(), new MapCacheEventCodec(codec, osType));
            return topic.addListener(List.class, new MessageListener<List<Object>>() {
                @Override
                public void onMessage(CharSequence channel, List<Object> msg) {
                    EntryEvent<K, V> event = new EntryEvent<K, V>(RedissonMapCache.this, EntryEvent.Type.EXPIRED, (K) msg.get(0), (V) msg.get(1), null);
                    ((EntryExpiredListener<K, V>) listener).onExpired(event);
                }
            });
        }

        throw new IllegalArgumentException("Wrong listener type " + listener.getClass());
    }

    @Override
    public void removeListener(int listenerId) {
        super.removeListener(listenerId);
        
        RTopic removedTopic = redisson.getTopic(getRemovedChannelName());
        removedTopic.removeListener(listenerId);

        RTopic createdTopic = redisson.getTopic(getCreatedChannelName());
        createdTopic.removeListener(listenerId);

        RTopic updatedTopic = redisson.getTopic(getUpdatedChannelName());
        updatedTopic.removeListener(listenerId);

        RTopic expiredTopic = redisson.getTopic(getExpiredChannelName());
        expiredTopic.removeListener(listenerId);
    }

    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        List<Object> keys = Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName());
        return super.sizeInMemoryAsync(keys);
    }

    @Override
    public void clear() {
        RFuture<Boolean> future = deleteAsync(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName());
        get(future);
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return deleteAsync(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName());
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size')); " +
                        "if maxSize ~= nil and maxSize ~= 0 then " +
                        "    redis.call('pexpire', KEYS[5], ARGV[1]); " +
                        "    redis.call('zadd', KEYS[4], 92233720368547758, 'redisson__expiretag'); " +
                        "    redis.call('pexpire', KEYS[4], ARGV[1]); " +
                        "end; " +
                        "redis.call('zadd', KEYS[2], 92233720368547758, 'redisson__expiretag'); " +
                        "redis.call('pexpire', KEYS[2], ARGV[1]); " +
                        "redis.call('zadd', KEYS[3], 92233720368547758, 'redisson__expiretag'); " +
                        "redis.call('pexpire', KEYS[3], ARGV[1]); " +
                        "return redis.call('pexpire', KEYS[1], ARGV[1]); ",
                Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()),
                timeUnit.toMillis(timeToLive));
    }

    @Override
    public RFuture<Boolean> expireAtAsync(long timestamp) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size')); " +
                        "if maxSize ~= nil and maxSize ~= 0 then " +
                        "    redis.call('pexpire', KEYS[5], ARGV[1]); " +
                        "    redis.call('zadd', KEYS[4], 92233720368547758, 'redisson__expiretag'); " +
                        "    redis.call('pexpire', KEYS[4], ARGV[1]); " +
                        "end; " +
                        "redis.call('zadd', KEYS[2], 92233720368547758, 'redisson__expiretag'); " +
                        "redis.call('pexpireat', KEYS[2], ARGV[1]); " +
                        "redis.call('zadd', KEYS[3], 92233720368547758, 'redisson__expiretag'); " +
                        "redis.call('pexpire', KEYS[3], ARGV[1]); " +
                        "return redis.call('pexpireat', KEYS[1], ARGV[1]); ",
                Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()),
                timestamp);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size')); " +
                        "if maxSize ~= nil and maxSize ~= 0 then " +
                        "    redis.call('persist', KEYS[5]); " +
                        "    redis.call('zrem', KEYS[4], 92233720368547758, 'redisson__expiretag'); " +
                        "    redis.call('persist', KEYS[4]); " +
                        "end; " +
                
                        "redis.call('zrem', KEYS[2], 'redisson__expiretag'); " +
                        "redis.call('persist', KEYS[2]); " +
                        "redis.call('zrem', KEYS[3], 'redisson__expiretag'); " +
                        "redis.call('persist', KEYS[3]); " +
                        "return redis.call('persist', KEYS[1]); ",
                Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()));
    }

    @Override
    public RFuture<Set<K>> readAllKeySetAsync() {
        return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_MAP_KEY_SET,
                "local s = redis.call('hgetall', KEYS[1]); " + 
                "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size'));"
                        + "local result = {}; "
                        + "for i, v in ipairs(s) do "
                            + "if i % 2 == 0 then "
                                + "local t, val = struct.unpack('dLc0', v); "
                                + "local key = s[i-1];" +
                                "local expireDate = 92233720368547758; " +
                                "local expireDateScore = redis.call('zscore', KEYS[2], key); "
                                + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                                + "end; "
                                + "if t ~= 0 then "
                                + "local expireIdle = redis.call('zscore', KEYS[3], key); "
                                + "if expireIdle ~= false then "
                                + "if tonumber(expireIdle) > tonumber(ARGV[1]) then "
                                    + "redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), key); "
                                    + "if maxSize ~= nil and maxSize ~= 0 then " +
                                        "local mode = redis.call('hget', KEYS[5], 'mode'); " +
                                        "if mode == false or mode == 'LRU' then " +
                                            "redis.call('zadd', KEYS[4], tonumber(ARGV[1]), key); " +
                                        "else " +
                                            "redis.call('zincrby', KEYS[4], 1, key); " +
                                        "end; "
                                    + "end; "
                                + "end; "
                                + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                + "end; "
                                + "end; "
                                + "if expireDate > tonumber(ARGV[1]) then "
                                    + "table.insert(result, key); "
                                + "end; "
                            + "end; "
                        + "end;" +
                        "return result;",
                Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()),
                System.currentTimeMillis());
    }

    @Override
    public RFuture<Set<K>> randomKeysAsync(int count) {
        return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_MAP_KEY_SET,
            "local s = redis.call('hrandfield', KEYS[1], ARGV[2], 'withvalues'); " +
                  "if s == false then " +
                       "return {};" +
                  "end; " +
                  "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size'));"
                        + "local result = {}; "
                        + "for i, v in ipairs(s) do "
                            + "if i % 2 == 0 then "
                                + "local t, val = struct.unpack('dLc0', v); "
                                + "local key = s[i-1];" +
                                "local expireDate = 92233720368547758; " +
                                "local expireDateScore = redis.call('zscore', KEYS[2], key); "
                                + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                                + "end; "
                                + "if t ~= 0 then "
                                + "local expireIdle = redis.call('zscore', KEYS[3], key); "
                                + "if expireIdle ~= false then "
                                + "if tonumber(expireIdle) > tonumber(ARGV[1]) then "
                                    + "redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), key); "
                                    + "if maxSize ~= nil and maxSize ~= 0 then " +
                                        "local mode = redis.call('hget', KEYS[5], 'mode'); " +
                                        "if mode == false or mode == 'LRU' then " +
                                            "redis.call('zadd', KEYS[4], tonumber(ARGV[1]), key); " +
                                        "else " +
                                            "redis.call('zincrby', KEYS[4], 1, key); " +
                                        "end; "
                                    + "end; "
                                + "end; "
                                + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                + "end; "
                                + "end; "
                                + "if expireDate > tonumber(ARGV[1]) then "
                                    + "table.insert(result, key); "
                                + "end; "
                            + "end; "
                        + "end;" +
                        "return result;",
                Arrays.asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()),
                System.currentTimeMillis(), count);
    }

    @Override
    public RFuture<Map<K, V>> randomEntriesAsync(int count) {
        return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_MAP,
            "local s = redis.call('hrandfield', KEYS[1], ARGV[2], 'withvalues'); " +
                "if s == false then " +
                    "return {};" +
                "end; "
                + "local result = {}; "
                + "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size'));"
                + "for i, v in ipairs(s) do "
                    + "if i % 2 == 0 then "
                      + "local t, val = struct.unpack('dLc0', v); "
                      + "local key = s[i-1];" +
                        "local expireDate = 92233720368547758; " +
                        "local expireDateScore = redis.call('zscore', KEYS[2], key); "
                        + "if expireDateScore ~= false then "
                            + "expireDate = tonumber(expireDateScore) "
                        + "end; "
                        + "if t ~= 0 then "
                            + "local expireIdle = redis.call('zscore', KEYS[3], key); "
                            + "if expireIdle ~= false then "
                                + "if tonumber(expireIdle) > tonumber(ARGV[1]) then "
                                    + "redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), key); "
                                            + "if maxSize ~= nil and maxSize ~= 0 then " +
                                                    "local mode = redis.call('hget', KEYS[5], 'mode'); " +
                                                    "if mode == false or mode == 'LRU' then " +
                                                        "redis.call('zadd', KEYS[4], tonumber(ARGV[1]), key); " +
                                                    "else " +
                                                        "redis.call('zincrby', KEYS[4], 1, key); " +
                                                    "end; "
                                            + "end; "
                                        + "end; "
                                        + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                    + "end; "
                                + "end; "
                                + "if expireDate > tonumber(ARGV[1]) then "
                                    + "table.insert(result, key); "
                                    + "table.insert(result, val); "
                                + "end; "
                            + "end; "
                        + "end;" +
                        "return result;",
                Arrays.asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()),
                System.currentTimeMillis(), count);
    }

    @Override
    public RFuture<Set<java.util.Map.Entry<K, V>>> readAllEntrySetAsync() {
        return readAll(RedisCommands.EVAL_MAP_ENTRY);
    }

    private <R> RFuture<R> readAll(RedisCommand<?> evalCommandType) {
        return commandExecutor.evalWriteAsync(getName(), codec, evalCommandType,
                "local s = redis.call('hgetall', KEYS[1]); "
                        + "local result = {}; "
                        + "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size'));"
                + "for i, v in ipairs(s) do "
                    + "if i % 2 == 0 then "
                      + "local t, val = struct.unpack('dLc0', v); "
                      + "local key = s[i-1];" +
                        "local expireDate = 92233720368547758; " +
                        "local expireDateScore = redis.call('zscore', KEYS[2], key); "
                        + "if expireDateScore ~= false then "
                            + "expireDate = tonumber(expireDateScore) "
                        + "end; "
                        + "if t ~= 0 then "
                            + "local expireIdle = redis.call('zscore', KEYS[3], key); "
                            + "if expireIdle ~= false then "
                                + "if tonumber(expireIdle) > tonumber(ARGV[1]) then "
                                    + "redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), key); "
                                            + "if maxSize ~= nil and maxSize ~= 0 then " +
                                                    "local mode = redis.call('hget', KEYS[5], 'mode'); " +
                                                    "if mode == false or mode == 'LRU' then " +
                                                        "redis.call('zadd', KEYS[4], tonumber(ARGV[1]), key); " +
                                                    "else " +
                                                        "redis.call('zincrby', KEYS[4], 1, key); " +
                                                    "end; "
                                            + "end; "
                                        + "end; "
                                        + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                                    + "end; "
                                + "end; "
                                + "if expireDate > tonumber(ARGV[1]) then "
                                    + "table.insert(result, key); "
                                    + "table.insert(result, val); "
                                + "end; "
                            + "end; "
                        + "end;" +
                        "return result;",
                Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()),
                System.currentTimeMillis());
    }

    @Override
    public RFuture<Map<K, V>> readAllMapAsync() {
        return readAll(RedisCommands.EVAL_MAP);
    }

    
    @Override
    public RFuture<Collection<V>> readAllValuesAsync() {
        return commandExecutor.evalWriteAsync(getName(), codec, RedisCommands.EVAL_MAP_VALUE_LIST,
                "local s = redis.call('hgetall', KEYS[1]); "
                    + "local result = {}; "
                    + "local maxSize = tonumber(redis.call('hget', KEYS[5], 'max-size')); "
                    + "for i, v in ipairs(s) do "
                        + "if i % 2 == 0 then "
                            + "local t, val = struct.unpack('dLc0', v); "
                            + "local key = s[i-1];" +
                            "local expireDate = 92233720368547758; " +
                            "local expireDateScore = redis.call('zscore', KEYS[2], key); "
                            + "if expireDateScore ~= false then "
                                + "expireDate = tonumber(expireDateScore) "
                            + "end; "
                            + "if t ~= 0 then "
                            + "local expireIdle = redis.call('zscore', KEYS[3], key); "
                            + "if expireIdle ~= false then "
                                + "if tonumber(expireIdle) > tonumber(ARGV[1]) then "
                                    + "redis.call('zadd', KEYS[3], t + tonumber(ARGV[1]), key); "
                                    + "if maxSize ~= nil and maxSize ~= 0 then " +
                                        "local mode = redis.call('hget', KEYS[5], 'mode'); " +
                                        "if mode == false or mode == 'LRU' then " +
                                            "redis.call('zadd', KEYS[4], tonumber(ARGV[1]), key); " +
                                        "else " +
                                            "redis.call('zincrby', KEYS[4], 1, key); " +
                                        "end; "
                                    + "end; "
                                + "end; "
                                + "expireDate = math.min(expireDate, tonumber(expireIdle)) "
                            + "end; "
                        + "end; "
                        + "if expireDate > tonumber(ARGV[1]) then "
                            + "table.insert(result, val); "
                        + "end; "
                        + "end; "
                    + "end;" +
                    "return result;",
                Arrays.<Object>asList(getName(), getTimeoutSetName(), getIdleSetName(), getLastAccessTimeSetName(), getOptionsName()),
                System.currentTimeMillis());
    }

    @Override
    public void destroy() {
        if (evictionScheduler != null) {
            evictionScheduler.remove(getName());
        }
        if (writeBehindService != null) {
            writeBehindService.stop(getName());
        }
    }
}
