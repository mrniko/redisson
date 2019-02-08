/**
 * Copyright 2018 Nikita Koksharov
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RFuture;
import org.redisson.api.RStream;
import org.redisson.api.StreamId;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandAsyncExecutor;

/**
 * 
 * @author Nikita Koksharov
 *
 * @param <K> key type
 * @param <V> value type
 */
public class RedissonStream<K, V> extends RedissonExpirable implements RStream<K, V> {

    public RedissonStream(Codec codec, CommandAsyncExecutor connectionManager, String name) {
        super(codec, connectionManager, name);
    }

    public RedissonStream(CommandAsyncExecutor connectionManager, String name) {
        super(connectionManager, name);
    }

    protected void checkKey(Object key) {
        if (key == null) {
            throw new NullPointerException("key can't be null");
        }
    }

    protected void checkValue(Object value) {
        if (value == null) {
            throw new NullPointerException("value can't be null");
        }
    }

    @Override
    public StreamId addAll(Map<K, V> entries) {
        return addAll(entries, 0, false);
    }
    
    @Override
    public RFuture<StreamId> addAllAsync(Map<K, V> entries) {
        return addAllAsync(entries, 0, false);
    }
    
    @Override
    public void addAll(StreamId id, Map<K, V> entries) {
        addAll(id, entries, 0, false);
    }

    @Override
    public RFuture<Void> addAllAsync(StreamId id, Map<K, V> entries) {
        return addAllAsync(id, entries, 0, false);
    }

    @Override
    public StreamId addAll(Map<K, V> entries, int trimLen, boolean trimStrict) {
        return get(addAllAsync(entries, trimLen, trimStrict));
    }
    
    @Override
    public RFuture<StreamId> addAllAsync(Map<K, V> entries, int trimLen, boolean trimStrict) {
        return addAllCustomAsync(null, entries, trimLen, trimStrict);
    }
    
    @Override
    public void addAll(StreamId id, Map<K, V> entries, int trimLen, boolean trimStrict) {
        get(addAllAsync(id, entries, trimLen, trimStrict));
    }

    private <R> RFuture<R> addAllCustomAsync(StreamId id, Map<K, V> entries, int trimLen, boolean trimStrict) {
        List<Object> params = new ArrayList<Object>(entries.size()*2 + 1);
        params.add(getName());
        
        if (trimLen > 0) {
            params.add("MAXLEN");
            if (!trimStrict) {
                params.add("~");
            }
            params.add(trimLen);
        }
        
        if (id == null) {
            params.add("*");
        } else {
            params.add(id.toString());
        }
        
        for (java.util.Map.Entry<? extends K, ? extends V> t : entries.entrySet()) {
            checkKey(t.getKey());
            checkValue(t.getValue());

            params.add(encodeMapKey(t.getKey()));
            params.add(encodeMapValue(t.getValue()));
        }

        if (id == null) {
            return commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.XADD, params.toArray());
        }
        return commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.XADD_VOID, params.toArray());
    }
    
    @Override
    public RFuture<Void> addAllAsync(StreamId id, Map<K, V> entries, int trimLen, boolean trimStrict) {
        return addAllCustomAsync(id, entries, trimLen, trimStrict);
    }

    @Override
    public long size() {
        return get(sizeAsync());
    }
    
    @Override
    public RFuture<Long> sizeAsync() {
        return commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.XLEN, getName());
    }
    
    @Override
    public Map<StreamId, Map<K, V>> read(int count, StreamId ... ids) {
        return get(readAsync(count, ids));
    }
    
    @Override
    public RFuture<Map<String, Map<StreamId, Map<K, V>>>> readAsync(int count, Collection<String> keys, StreamId ... ids) {
        return readAsync(count, -1, null, keys, ids);
    }

    @Override
    public Map<StreamId, Map<K, V>> read(int count, long timeout, TimeUnit unit, StreamId... ids) {
        return get(readAsync(count, timeout, unit, ids));
    }

    @Override
    public RFuture<Map<String, Map<StreamId, Map<K, V>>>> readAsync(int count, long timeout, TimeUnit unit, Collection<String> keys, StreamId... ids) {
        if (keys.size() + 1 != ids.length) {
            throw new IllegalArgumentException("keys amount should be lower by one than ids amount");
        }
        
        List<Object> params = new ArrayList<Object>();
        if (count > 0) {
            params.add("COUNT");
            params.add(count);
        }
        
        if (timeout > 0) {
            params.add("BLOCK");
            params.add(toSeconds(timeout, unit)*1000);
        }
        
        params.add("STREAMS");
        params.add(getName());
        if (keys != null) {
            for (String key : keys) {
                params.add(key);
            }
        }
        
        for (StreamId id : ids) {
            params.add(id.toString());
        }

        if (timeout > 0) {
            return commandExecutor.readAsync(getName(), codec, RedisCommands.XREAD_BLOCKING, params.toArray());
        }
        return commandExecutor.readAsync(getName(), codec, RedisCommands.XREAD, params.toArray());
    }

    @Override
    public RFuture<StreamId> addAsync(K key, V value) {
        return addAsync(key, value, 0, false);
    }

    @Override
    public RFuture<Void> addAsync(StreamId id, K key, V value) {
        return addAsync(id, key, value, 0, false);
    }

    @Override
    public RFuture<StreamId> addAsync(K key, V value, int trimLen, boolean trimStrict) {
        return addCustomAsync(null, key, value, trimLen, trimStrict);
    }

    private <R> RFuture<R> addCustomAsync(StreamId id, K key, V value, int trimLen, boolean trimStrict) {
        List<Object> params = new LinkedList<Object>();
        params.add(getName());
        
        if (trimLen > 0) {
            params.add("MAXLEN");
            if (!trimStrict) {
                params.add("~");
            }
            params.add(trimLen);
        }
        
        if (id == null) {
            params.add("*");
        } else {
            params.add(id.toString());
        }
        
        checkKey(key);
        checkValue(value);

        params.add(encodeMapKey(key));
        params.add(encodeMapValue(value));

        if (id == null) {
            return commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.XADD, params.toArray());
        }
        return commandExecutor.writeAsync(getName(), StringCodec.INSTANCE, RedisCommands.XADD_VOID, params.toArray());
    }
    
    @Override
    public RFuture<Void> addAsync(StreamId id, K key, V value, int trimLen, boolean trimStrict) {
        return addCustomAsync(id, key, value, trimLen, trimStrict);
    }

    @Override
    public StreamId add(K key, V value) {
        return get(addAsync(key, value));
    }

    @Override
    public void add(StreamId id, K key, V value) {
        get(addAsync(id, key, value));
    }

    @Override
    public StreamId add(K key, V value, int trimLen, boolean trimStrict) {
        return get(addAsync(key, value, trimLen, trimStrict));
    }

    @Override
    public void add(StreamId id, K key, V value, int trimLen, boolean trimStrict) {
        get(addAsync(id, key, value, trimLen, trimStrict));
    }

    @Override
    public RFuture<Map<StreamId, Map<K, V>>> readAsync(int count, StreamId... ids) {
        return readAsync(count, 0, null, ids);
    }

    @Override
    public RFuture<Map<StreamId, Map<K, V>>> readAsync(int count, long timeout, TimeUnit unit,
            StreamId... ids) {
        List<Object> params = new ArrayList<Object>();
        if (count > 0) {
            params.add("COUNT");
            params.add(count);
        }
        
        if (timeout > 0) {
            params.add("BLOCK");
            params.add(toSeconds(timeout, unit)*1000);
        }
        
        params.add("STREAMS");
        params.add(getName());

        for (StreamId id : ids) {
            params.add(id.toString());
        }

        if (timeout > 0) {
            return commandExecutor.readAsync(getName(), codec, RedisCommands.XREAD_BLOCKING_SINGLE, params.toArray());
        }
        return commandExecutor.readAsync(getName(), codec, RedisCommands.XREAD_SINGLE, params.toArray());
    }

    @Override
    public Map<String, Map<StreamId, Map<K, V>>> read(int count, Collection<String> keys, StreamId... ids) {
        return get(readAsync(count, keys, ids));
    }

    @Override
    public Map<String, Map<StreamId, Map<K, V>>> read(int count, long timeout, TimeUnit unit, Collection<String> keys,
            StreamId... ids) {
        return get(readAsync(count, timeout, unit, keys, ids));
    }

    @Override
    public RFuture<Map<StreamId, Map<K, V>>> rangeAsync(int count, StreamId startId, StreamId endId) {
        List<Object> params = new LinkedList<Object>();
        params.add(getName());
        params.add(startId);
        params.add(endId);
        
        if (count > 0) {
            params.add("COUNT");
            params.add(count);
        }
        
        return commandExecutor.readAsync(getName(), codec, RedisCommands.XRANGE, params.toArray());
    }

    @Override
    public Map<StreamId, Map<K, V>> range(int count, StreamId startId, StreamId endId) {
        return get(rangeAsync(count, startId, endId));
    }

    @Override
    public RFuture<Map<StreamId, Map<K, V>>> rangeReversedAsync(int count, StreamId startId, StreamId endId) {
        List<Object> params = new LinkedList<Object>();
        params.add(getName());
        params.add(startId);
        params.add(endId);
        
        if (count > 0) {
            params.add("COUNT");
            params.add(count);
        }
        
        return commandExecutor.readAsync(getName(), codec, RedisCommands.XREVRANGE, params.toArray());
    }

    @Override
    public Map<StreamId, Map<K, V>> rangeReversed(int count, StreamId startId, StreamId endId) {
        return get(rangeReversedAsync(count, startId, endId));
    }

    @Override
    public RFuture<Map<StreamId, Map<K, V>>> readAsync(StreamId... ids) {
        return readAsync(0, ids);
    }

    @Override
    public RFuture<Map<StreamId, Map<K, V>>> readAsync(long timeout, TimeUnit unit, StreamId... ids) {
        return readAsync(0, timeout, unit, ids);
    }

    @Override
    public RFuture<Map<String, Map<StreamId, Map<K, V>>>> readAsync(Collection<String> keys, StreamId... ids) {
        return readAsync(0, keys, ids);
    }

    @Override
    public RFuture<Map<String, Map<StreamId, Map<K, V>>>> readAsync(long timeout, TimeUnit unit,
            Collection<String> keys, StreamId... ids) {
        return readAsync(0, timeout, unit, keys, ids);
    }

    @Override
    public RFuture<Map<StreamId, Map<K, V>>> rangeAsync(StreamId startId, StreamId endId) {
        return rangeAsync(0, startId, endId);
    }

    @Override
    public RFuture<Map<StreamId, Map<K, V>>> rangeReversedAsync(StreamId startId, StreamId endId) {
        return rangeReversedAsync(0, startId, endId);
    }

    @Override
    public Map<StreamId, Map<K, V>> read(StreamId... ids) {
        return read(0, ids);
    }

    @Override
    public Map<StreamId, Map<K, V>> read(long timeout, TimeUnit unit, StreamId... ids) {
        return read(0, timeout, unit, ids);
    }

    @Override
    public Map<String, Map<StreamId, Map<K, V>>> read(Collection<String> keys, StreamId... ids) {
        return read(0, keys, ids);
    }

    @Override
    public Map<String, Map<StreamId, Map<K, V>>> read(long timeout, TimeUnit unit, Collection<String> keys,
            StreamId... ids) {
        return read(0, timeout, unit, keys, ids);
    }

    @Override
    public Map<StreamId, Map<K, V>> range(StreamId startId, StreamId endId) {
        return range(0, startId, endId);
    }

    @Override
    public Map<StreamId, Map<K, V>> rangeReversed(StreamId startId, StreamId endId) {
        return rangeReversed(0, startId, endId);
    }
    
}
