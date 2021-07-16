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

import io.netty.buffer.ByteBuf;
import org.redisson.api.*;
import org.redisson.client.codec.ByteArrayCodec;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.CountableListener;
import org.redisson.misc.Hash;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Base Redisson object
 *
 * @author Nikita Koksharov
 *
 */
public abstract class RedissonObject implements RObject {

    protected final CommandAsyncExecutor commandExecutor;
    protected String name;
    protected final Codec codec;

    public RedissonObject(Codec codec, CommandAsyncExecutor commandExecutor, String name) {
        this.codec = codec;
        this.commandExecutor = commandExecutor;
        if (name == null) {
            throw new NullPointerException("name can't be null");
        }

        setName(name);
    }

    public RedissonObject(CommandAsyncExecutor commandExecutor, String name) {
        this(commandExecutor.getConnectionManager().getCodec(), commandExecutor, name);
    }

    public static String prefixName(String prefix, String name) {
        if (name.contains("{")) {
            return prefix + ":" + name;
        }
        return prefix + ":{" + name + "}";
    }
    
    public static String suffixName(String name, String suffix) {
        if (name.contains("{")) {
            return name + ":" + suffix;
        }
        return "{" + name + "}:" + suffix;
    }

    protected final <T> Stream<T> toStream(Iterator<T> iterator) {
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, false);
    }
    
    protected final <V> V get(RFuture<V> future) {
        return commandExecutor.get(future);
    }
    
    protected final long toSeconds(long timeout, TimeUnit unit) {
        long seconds = unit.toSeconds(timeout);
        if (timeout != 0 && seconds == 0) {
            seconds = 1;
        }
        return seconds;
    }

    @Override
    public String getName() {
        return commandExecutor.getConnectionManager().getConfig().getNameMapper().unmap(name);
    }

    public final String getRawName() {
        return name;
    }

    protected String getRawName(Object o) {
        return getRawName();
    }

    protected final void setName(String name) {
        this.name = commandExecutor.getConnectionManager().getConfig().getNameMapper().map(name);
    }

    @Override
    public void rename(String newName) {
        get(renameAsync(newName));
    }
    
    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.MEMORY_USAGE, getRawName());
    }
    
    public final RFuture<Long> sizeInMemoryAsync(List<Object> keys) {
        return sizeInMemoryAsync(commandExecutor, keys);
    }

    public final RFuture<Long> sizeInMemoryAsync(CommandAsyncExecutor commandExecutor, List<Object> keys) {
        return commandExecutor.evalWriteAsync((String) keys.get(0), StringCodec.INSTANCE, RedisCommands.EVAL_LONG,
                  "local total = 0;"
                + "for j = 1, #KEYS, 1 do "
                    + "local size = redis.call('memory', 'usage', KEYS[j]); "
                    + "if size ~= false then "
                        + "total = total + size;"
                    + "end; "
                + "end; "
                + "return total; ", keys);

    }
    
    @Override
    public long sizeInMemory() {
        return get(sizeInMemoryAsync());
    }

    @Override
    public RFuture<Void> renameAsync(String newName) {
        RFuture<Void> f = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.RENAME, getRawName(), newName);
        f.onComplete((r, e) -> {
            if (e == null) {
                setName(newName);
            }
        });
        return f;
    }

    @Override
    public void migrate(String host, int port, int database, long timeout) {
        get(migrateAsync(host, port, database, timeout));
    }

    @Override
    public RFuture<Void> migrateAsync(String host, int port, int database, long timeout) {
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.MIGRATE, host, port, getRawName(), database, timeout);
    }
    
    @Override
    public void copy(String host, int port, int database, long timeout) {
        get(copyAsync(host, port, database, timeout));
    }

    @Override
    public RFuture<Void> copyAsync(String host, int port, int database, long timeout) {
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.MIGRATE, host, port, getRawName(), database, timeout, "COPY");
    }
    
    @Override
    public boolean move(int database) {
        return get(moveAsync(database));
    }

    @Override
    public RFuture<Boolean> moveAsync(int database) {
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.MOVE, getRawName(), database);
    }

    @Override
    public boolean renamenx(String newName) {
        return get(renamenxAsync(newName));
    }

    @Override
    public RFuture<Boolean> renamenxAsync(String newName) {
        RFuture<Boolean> f = commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.RENAMENX, getRawName(), newName);
        f.onComplete((value, e) -> {
            if (e == null && value) {
                setName(newName);
            }
        });
        return f;

    }

    @Override
    public boolean delete() {
        return get(deleteAsync());
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.DEL_BOOL, getRawName());
    }

    protected RFuture<Boolean> deleteAsync(String... keys) {
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.DEL_OBJECTS, keys);
    }

    @Override
    public boolean unlink() {
        return get(unlinkAsync());
    }

    @Override
    public RFuture<Boolean> unlinkAsync() {
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.UNLINK_BOOL, getRawName());
    }

    @Override
    public boolean touch() {
        return get(touchAsync());
    }

    @Override
    public RFuture<Boolean> touchAsync() {
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.TOUCH, getRawName());
    }
    
    @Override
    public boolean isExists() {
        return get(isExistsAsync());
    }

    @Override
    public RFuture<Boolean> isExistsAsync() {
        return commandExecutor.readAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.EXISTS, getRawName());
    }

    @Override
    public Codec getCodec() {
        return codec;
    }

    protected List<ByteBuf> encode(Object... values) {
        List<ByteBuf> result = new ArrayList<>(values.length);
        for (Object object : values) {
            result.add(encode(object));
        }
        return result;
    }

    protected List<ByteBuf> encode(Collection<?> values) {
        List<ByteBuf> result = new ArrayList<>(values.size());
        for (Object object : values) {
            result.add(encode(object));
        }
        return result;
    }
    
    public void encode(Collection<Object> params, Collection<?> values) {
        for (Object object : values) {
            params.add(encode(object));
        }
    }
    
    public String getLockByMapKey(Object key, String suffix) {
        ByteBuf keyState = encodeMapKey(key);
        try {
            return suffixName(getRawName(key), Hash.hash128toBase64(keyState) + ":" + suffix);
        } finally {
            keyState.release();
        }
    }

    public String getLockByValue(Object key, String suffix) {
        ByteBuf keyState = encode(key);
        try {
            return suffixName(getRawName(key), Hash.hash128toBase64(keyState) + ":" + suffix);
        } finally {
            keyState.release();
        }
    }
    
    protected void encodeMapKeys(Collection<Object> params, Collection<?> values) {
        for (Object object : values) {
            params.add(encodeMapKey(object));
        }
    }

    protected void encodeMapValues(Collection<Object> params, Collection<?> values) {
        for (Object object : values) {
            params.add(encodeMapValue(object));
        }
    }
    
    public ByteBuf encode(Object value) {
        return commandExecutor.encode(codec, value);
    }
    
    public ByteBuf encodeMapKey(Object value) {
        return commandExecutor.encodeMapKey(codec, value);
    }

    public ByteBuf encodeMapValue(Object value) {
        return commandExecutor.encodeMapValue(codec, value);
    }

    @Override
    public byte[] dump() {
        return get(dumpAsync());
    }
    
    @Override
    public RFuture<byte[]> dumpAsync() {
        return commandExecutor.readAsync(getRawName(), ByteArrayCodec.INSTANCE, RedisCommands.DUMP, getRawName());
    }
    
    @Override
    public void restore(byte[] state) {
        get(restoreAsync(state));
    }
    
    @Override
    public RFuture<Void> restoreAsync(byte[] state) {
        return restoreAsync(state, 0, null);
    }
    
    @Override
    public void restore(byte[] state, long timeToLive, TimeUnit timeUnit) {
        get(restoreAsync(state, timeToLive, timeUnit));
    }
    
    @Override
    public RFuture<Void> restoreAsync(byte[] state, long timeToLive, TimeUnit timeUnit) {
        long ttl = 0;
        if (timeToLive > 0) {
            ttl = timeUnit.toMillis(timeToLive);
        }
        
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.RESTORE, getRawName(), ttl, state);
    }

    @Override
    public void restoreAndReplace(byte[] state, long timeToLive, TimeUnit timeUnit) {
        get(restoreAndReplaceAsync(state, timeToLive, timeUnit));
    }
    
    @Override
    public RFuture<Void> restoreAndReplaceAsync(byte[] state, long timeToLive, TimeUnit timeUnit) {
        long ttl = 0;
        if (timeToLive > 0) {
            ttl = timeUnit.toMillis(timeToLive);
        }
        
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.RESTORE, getRawName(), ttl, state, "REPLACE");
    }
    
    @Override
    public void restoreAndReplace(byte[] state) {
        get(restoreAndReplaceAsync(state));
    }
    
    @Override
    public RFuture<Void> restoreAndReplaceAsync(byte[] state) {
        return restoreAndReplaceAsync(state, 0, null);
    }

    public Long getIdleTime() {
        return get(getIdleTimeAsync());
    }

    @Override
    public RFuture<Long> getIdleTimeAsync() {
        return commandExecutor.writeAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.OBJECT_IDLETIME, getRawName());
    }

    protected final <T extends ObjectListener> int addListener(String name, T listener, BiConsumer<T, String> consumer, Integer previous) {
        int next = addListener(name, listener, consumer);
        if (previous != null && previous == next) {
            return next;
        }
        throw new UnsupportedOperationException("Incompatible listener combination");
    }

    protected final <T extends ObjectListener> int addListener(String name, T listener, BiConsumer<T, String> consumer) {
        RPatternTopic topic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, name);
        return topic.addListener(String.class, (pattern, channel, msg) -> {
            if (msg.equals(getRawName())) {
                consumer.accept(listener, msg);
            }
        });
    }

    protected final <T extends ObjectListener> RFuture<Integer> addListenerAsync(String name, T listener, BiConsumer<T, String> consumer) {
        RPatternTopic topic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, name);
        return topic.addListenerAsync(String.class, (pattern, channel, msg) -> {
            if (msg.equals(getRawName())) {
                consumer.accept(listener, msg);
            }
        });
    }

    public int addListener(ObjectListener listener, Integer previous) {
        if (listener instanceof ExpiredObjectListener) {
            previous = addListener("__keyevent@*:expired", (ExpiredObjectListener) listener, ExpiredObjectListener::onExpired, previous);
        }
        if (listener instanceof DeletedObjectListener) {
            previous = addListener("__keyevent@*:del", (DeletedObjectListener) listener, DeletedObjectListener::onDeleted, previous);
        }
        if (previous == null) {
            throw new IllegalArgumentException();
        }
        return previous;
    }

    @Override
    public int addListener(ObjectListener listener) {
        return addListener(listener, null);
    }
    
    @Override
    public RFuture<Integer> addListenerAsync(ObjectListener listener) {
        if (listener instanceof ExpiredObjectListener) {
            return addListenerAsync("__keyevent@*:expired", (ExpiredObjectListener) listener, ExpiredObjectListener::onExpired);
        }
        if (listener instanceof DeletedObjectListener) {
            return addListenerAsync("__keyevent@*:del", (DeletedObjectListener) listener, DeletedObjectListener::onDeleted);
        }
        throw new IllegalArgumentException();
    }
    
    @Override
    public void removeListener(int listenerId) {
        RPatternTopic expiredTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:expired");
        expiredTopic.removeListener(listenerId);

        RPatternTopic deletedTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:del");
        deletedTopic.removeListener(listenerId);
    }
    
    @Override
    public RFuture<Void> removeListenerAsync(int listenerId) {
        RPromise<Void> result = new RedissonPromise<>();
        CountableListener<Void> listener = new CountableListener<>(result, null, 2);
        removeListenersAsync(listenerId, listener);
        return result;
    }

    protected final void removeListenersAsync(int listenerId, CountableListener<Void> listener) {
        RPatternTopic expiredTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:expired");
        expiredTopic.removeListenerAsync(listenerId).onComplete(listener);

        RPatternTopic deletedTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:del");
        deletedTopic.removeListenerAsync(listenerId).onComplete(listener);
    }

}
