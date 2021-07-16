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

import org.redisson.api.*;
import org.redisson.api.listener.*;
import org.redisson.api.mapreduce.RCollectionMapReduce;
import org.redisson.client.RedisException;
import org.redisson.client.codec.Codec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.convertor.BooleanNumberReplayConvertor;
import org.redisson.client.protocol.convertor.Convertor;
import org.redisson.client.protocol.convertor.IntegerReplayConvertor;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.iterator.RedissonListIterator;
import org.redisson.mapreduce.RedissonCollectionMapReduce;
import org.redisson.misc.CountableListener;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;

import java.util.*;
import java.util.function.Predicate;

import static org.redisson.client.protocol.RedisCommands.*;

/**
 * Distributed and concurrent implementation of {@link java.util.List}
 *
 * @author Nikita Koksharov
 *
 * @param <V> the type of elements held in this collection
 */
public class RedissonList<V> extends RedissonExpirable implements RList<V> {

    private RedissonClient redisson;
    
    public RedissonList(CommandAsyncExecutor commandExecutor, String name, RedissonClient redisson) {
        super(commandExecutor, name);
        this.redisson = redisson;
    }

    public RedissonList(Codec codec, CommandAsyncExecutor commandExecutor, String name, RedissonClient redisson) {
        super(codec, commandExecutor, name);
        this.redisson = redisson;
    }

    @Override
    public <KOut, VOut> RCollectionMapReduce<V, KOut, VOut> mapReduce() {
        return new RedissonCollectionMapReduce<V, KOut, VOut>(this, redisson, commandExecutor);
    }
    
    @Override
    public int size() {
        return get(sizeAsync());
    }

    public RFuture<Integer> sizeAsync() {
        return commandExecutor.readAsync(getRawName(), codec, LLEN_INT, getRawName());
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        return get(containsAsync(o));
    }

    @Override
    public Iterator<V> iterator() {
        return listIterator();
    }

    @Override
    public Object[] toArray() {
        List<V> list = readAll();
        return list.toArray();
    }

    @Override
    public List<V> readAll() {
        return get(readAllAsync());
    }

    @Override
    public RFuture<List<V>> readAllAsync() {
        return commandExecutor.readAsync(getRawName(), codec, LRANGE, getRawName(), 0, -1);
    }

    @Override
    public <T> T[] toArray(T[] a) {
        List<V> list = readAll();
        return list.toArray(a);
    }

    @Override
    public boolean add(V e) {
        return get(addAsync(e));
    }

    @Override
    public RFuture<Boolean> addAsync(V e) {
        return addAsync(e, RPUSH_BOOLEAN);
    }
    
    protected <T> RFuture<T> addAsync(V e, RedisCommand<T> command) {
        return commandExecutor.writeAsync(getRawName(), codec, command, getRawName(), encode(e));
    }

    @Override
    public boolean remove(Object o) {
        return get(removeAsync(o));
    }

    @Override
    public RFuture<Boolean> removeAsync(Object o) {
        return removeAsync(o, 1);
    }

    @Override
    public RFuture<Boolean> removeAsync(Object o, int count) {
        return commandExecutor.writeAsync(getRawName(), codec, LREM_SINGLE, getRawName(), count, encode(o));
    }

    @Override
    public boolean remove(Object o, int count) {
        return get(removeAsync(o, count));
    }

    @Override
    public RFuture<Boolean> containsAllAsync(Collection<?> c) {
        if (c.isEmpty()) {
            return RedissonPromise.newSucceededFuture(true);
        }

        return commandExecutor.evalReadAsync(getRawName(), codec, RedisCommands.EVAL_BOOLEAN,
                "local items = redis.call('lrange', KEYS[1], 0, -1) " +
                "for i=1, #items do " +
                    "for j = 1, #ARGV, 1 do " +
                        "if items[i] == ARGV[j] then " +
                            "table.remove(ARGV, j) " +
                        "end " +
                    "end " +
                "end " +
                "return #ARGV == 0 and 1 or 0",
                Collections.<Object>singletonList(getRawName()), encode(c).toArray());
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return get(containsAllAsync(c));
    }

    @Override
    public boolean addAll(Collection<? extends V> c) {
        return get(addAllAsync(c));
    }

    @Override
    public RFuture<Boolean> addAllAsync(Collection<? extends V> c) {
        if (c.isEmpty()) {
            return RedissonPromise.newSucceededFuture(false);
        }

        List<Object> args = new ArrayList<Object>(c.size() + 1);
        args.add(getRawName());
        encode(args, c);
        return commandExecutor.writeAsync(getRawName(), codec, RPUSH_BOOLEAN, args.toArray());
    }

    @Override
    public RFuture<Boolean> addAllAsync(int index, Collection<? extends V> coll) {
        if (index < 0) {
            throw new IndexOutOfBoundsException("index: " + index);
        }

        if (coll.isEmpty()) {
            return RedissonPromise.newSucceededFuture(false);
        }

        if (index == 0) { // prepend elements to list
            List<Object> elements = new ArrayList<Object>();
            encode(elements, coll);
            Collections.reverse(elements);
            elements.add(0, getRawName());

            return commandExecutor.writeAsync(getRawName(), codec, LPUSH_BOOLEAN, elements.toArray());
        }

        List<Object> args = new ArrayList<Object>(coll.size() + 1);
        args.add(index);
        encode(args, coll);
        
        return commandExecutor.evalWriteAsync(getRawName(), codec, RedisCommands.EVAL_BOOLEAN,
                "local ind = table.remove(ARGV, 1); " + // index is the first parameter
                        "local size = redis.call('llen', KEYS[1]); " +
                        "assert(tonumber(ind) <= size, 'index: ' .. ind .. ' but current size: ' .. size); " +
                        "local tail = redis.call('lrange', KEYS[1], ind, -1); " +
                        "redis.call('ltrim', KEYS[1], 0, ind - 1); " +
                        "for i=1, #ARGV, 5000 do "
                            + "redis.call('rpush', KEYS[1], unpack(ARGV, i, math.min(i+4999, #ARGV))); "
                        + "end " +
                        "if #tail > 0 then " +
                            "for i=1, #tail, 5000 do "
                                + "redis.call('rpush', KEYS[1], unpack(tail, i, math.min(i+4999, #tail))); "
                          + "end "
                      + "end;" +
                        "return 1;",
                Collections.<Object>singletonList(getRawName()), args.toArray());
    }

    @Override
    public boolean addAll(int index, Collection<? extends V> coll) {
        return get(addAllAsync(index, coll));
    }

    @Override
    public RFuture<Boolean> removeAllAsync(Collection<?> c) {
        if (c.isEmpty()) {
            return RedissonPromise.newSucceededFuture(false);
        }

        return commandExecutor.evalWriteAsync(getRawName(), codec, RedisCommands.EVAL_BOOLEAN,
                        "local v = 0 " +
                        "for i = 1, #ARGV, 1 do "
                            + "if redis.call('lrem', KEYS[1], 0, ARGV[i]) == 1 "
                            + "then v = 1 end "
                        +"end "
                       + "return v ",
                Collections.<Object>singletonList(getRawName()), encode(c).toArray());
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return get(removeAllAsync(c));
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return get(retainAllAsync(c));
    }

    @Override
    public RFuture<Boolean> retainAllAsync(Collection<?> c) {
        if (c.isEmpty()) {
            return deleteAsync();
        }

        return commandExecutor.evalWriteAsync(getRawName(), codec, RedisCommands.EVAL_BOOLEAN,
                "local changed = 0 " +
                "local items = redis.call('lrange', KEYS[1], 0, -1) "
                   + "local i = 1 "
                   + "while i <= #items do "
                        + "local element = items[i] "
                        + "local isInAgrs = false "
                        + "for j = 1, #ARGV, 1 do "
                            + "if ARGV[j] == element then "
                                + "isInAgrs = true "
                                + "break "
                            + "end "
                        + "end "
                        + "if isInAgrs == false then "
                            + "redis.call('LREM', KEYS[1], 0, element) "
                            + "changed = 1 "
                        + "end "
                        + "i = i + 1 "
                   + "end "
                   + "return changed ",
                Collections.<Object>singletonList(getRawName()), encode(c).toArray());
    }


    @Override
    public void clear() {
        delete();
    }

    @Override
    public RFuture<V> getAsync(int index) {
        return commandExecutor.readAsync(getRawName(), codec, LINDEX, getRawName(), index);
    }
    
    public List<V> get(int...indexes) {
        return get(getAsync(indexes));
    }
    
    public RFuture<List<V>> getAsync(int...indexes) {
        List<Integer> params = new ArrayList<Integer>();
        for (Integer index : indexes) {
            params.add(index);
        }
        return commandExecutor.evalReadAsync(getRawName(), codec, RedisCommands.EVAL_LIST,
                "local result = {}; " + 
                "for i = 1, #ARGV, 1 do "
                    + "local value = redis.call('lindex', KEYS[1], ARGV[i]);"
                    + "table.insert(result, value);" + 
                "end; " +
                "return result;",
                Collections.<Object>singletonList(getRawName()), params.toArray());
    }


    @Override
    public V get(int index) {
        return getValue(index);
    }

    V getValue(int index) {
        return get(getAsync(index));
    }

    @Override
    public V set(int index, V element) {
        try {
            return get(setAsync(index, element));
        } catch (RedisException e) {
            if (e.getCause() instanceof IndexOutOfBoundsException) {
                throw (IndexOutOfBoundsException) e.getCause();
            }
            throw e;
        }
    }

    @Override
    public RFuture<V> setAsync(int index, V element) {
        RPromise<V> result = new RedissonPromise<V>();
        RFuture<V> future = commandExecutor.evalWriteAsync(getRawName(), codec, RedisCommands.EVAL_OBJECT,
                "local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                        "redis.call('lset', KEYS[1], ARGV[1], ARGV[2]); " +
                        "return v",
                Collections.<Object>singletonList(getRawName()), index, encode(element));
        future.onComplete((res, e) -> {
            if (e != null) {
                if (e.getMessage().contains("ERR index out of range")) {
                    result.tryFailure(new IndexOutOfBoundsException("index out of range"));
                    return;
                }
                result.tryFailure(e);
                return;
            }
            
            result.trySuccess(res);
        });
        return result;
    }

    @Override
    public void fastSet(int index, V element) {
        get(fastSetAsync(index, element));
    }

    @Override
    public RFuture<Void> fastSetAsync(int index, V element) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.LSET, getRawName(), index, encode(element));
    }

    @Override
    public void add(int index, V element) {
        addAll(index, Collections.singleton(element));
    }
    
    @Override
    public RFuture<Boolean> addAsync(int index, V element) {
        return addAllAsync(index, Collections.singleton(element));
    }
    
    @Override
    public V remove(int index) {
        return get(removeAsync(index));
    }
    
    @Override
    public RFuture<V> removeAsync(int index) {
        if (index == 0) {
            return commandExecutor.writeAsync(getRawName(), codec, LPOP, getRawName());
        }

        return commandExecutor.evalWriteAsync(getRawName(), codec, EVAL_OBJECT,
                "local v = redis.call('lindex', KEYS[1], ARGV[1]); " +
                "redis.call('lset', KEYS[1], ARGV[1], 'DELETED_BY_REDISSON');" +
                "redis.call('lrem', KEYS[1], 1, 'DELETED_BY_REDISSON');" +
                "return v",
                Collections.<Object>singletonList(getRawName()), index);
    }

    
    @Override
    public void fastRemove(int index) {
        get(fastRemoveAsync(index));
    }
    
    @Override
    public RFuture<Void> fastRemoveAsync(int index) {
        return commandExecutor.evalWriteAsync(getRawName(), codec, RedisCommands.EVAL_VOID,
                "redis.call('lset', KEYS[1], ARGV[1], 'DELETED_BY_REDISSON');" +
                "redis.call('lrem', KEYS[1], 1, 'DELETED_BY_REDISSON');",
                Collections.<Object>singletonList(getRawName()), index);
    }
    
    @Override
    public int indexOf(Object o) {
        return get(indexOfAsync(o));
    }

    @Override
    public RFuture<Boolean> containsAsync(Object o) {
        return indexOfAsync(o, new BooleanNumberReplayConvertor(-1L));
    }

    public <R> RFuture<R> indexOfAsync(Object o, Convertor<R> convertor) {
        return commandExecutor.evalReadAsync(getRawName(), codec, new RedisCommand<R>("EVAL", convertor),
                "local key = KEYS[1] " +
                "local obj = ARGV[1] " +
                "local items = redis.call('lrange', key, 0, -1) " +
                "for i=1,#items do " +
                    "if items[i] == obj then " +
                        "return i - 1 " +
                    "end " +
                "end " +
                "return -1",
                Collections.<Object>singletonList(getRawName()), encode(o));
    }

    @Override
    public RFuture<Integer> indexOfAsync(Object o) {
        return indexOfAsync(o, new IntegerReplayConvertor());
    }

    @Override
    public int lastIndexOf(Object o) {
        return get(lastIndexOfAsync(o));
    }

    @Override
    public RFuture<Integer> lastIndexOfAsync(Object o) {
        return commandExecutor.evalReadAsync(getRawName(), codec, RedisCommands.EVAL_INTEGER,
                "local key = KEYS[1] " +
                "local obj = ARGV[1] " +
                "local items = redis.call('lrange', key, 0, -1) " +
                "for i = #items, 1, -1 do " +
                    "if items[i] == obj then " +
                        "return i - 1 " +
                    "end " +
                "end " +
                "return -1",
                Collections.<Object>singletonList(getRawName()), encode(o));
    }
    
    public <R> RFuture<R> lastIndexOfAsync(Object o, Convertor<R> convertor) {
        return commandExecutor.evalReadAsync(getRawName(), codec, new RedisCommand<R>("EVAL", convertor),
                "local key = KEYS[1] " +
                "local obj = ARGV[1] " +
                "local items = redis.call('lrange', key, 0, -1) " +
                "for i = #items, 1, -1 do " +
                    "if items[i] == obj then " +
                        "return i - 1 " +
                    "end " +
                "end " +
                "return -1",
                Collections.<Object>singletonList(getRawName()), encode(o));
    }

    @Override
    public void trim(int fromIndex, int toIndex) {
        get(trimAsync(fromIndex, toIndex));
    }

    @Override
    public RFuture<Void> trimAsync(int fromIndex, int toIndex) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.LTRIM, getRawName(), fromIndex, toIndex);
    }

    @Override
    public ListIterator<V> listIterator() {
        return listIterator(0);
    }

    @Override
    public ListIterator<V> listIterator(int ind) {
        return new RedissonListIterator<V>(ind) {

            @Override
            public V getValue(int index) {
                return RedissonList.this.getValue(index);
            }

            @Override
            public V remove(int index) {
                return RedissonList.this.remove(index);
            }

            @Override
            public void fastSet(int index, V value) {
                RedissonList.this.fastSet(index, value);
            }

            @Override
            public void add(int index, V value) {
                RedissonList.this.add(index, value);
            }
        };
    }

    @Override
    public RList<V> subList(int fromIndex, int toIndex) {
        int size = size();
        if (fromIndex < 0 || toIndex > size) {
            throw new IndexOutOfBoundsException("fromIndex: " + fromIndex + " toIndex: " + toIndex + " size: " + size);
        }
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException("fromIndex: " + fromIndex + " toIndex: " + toIndex);
        }

        return new RedissonSubList<V>(codec, commandExecutor, getRawName(), fromIndex, toIndex);
    }

    @Override
    @SuppressWarnings("AvoidInlineConditionals")
    public String toString() {
        Iterator<V> it = iterator();
        if (! it.hasNext())
            return "[]";

        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (;;) {
            V e = it.next();
            sb.append(e == this ? "(this Collection)" : e);
            if (! it.hasNext())
                return sb.append(']').toString();
            sb.append(',').append(' ');
        }
    }

    @Override
    @SuppressWarnings("AvoidInlineConditionals")
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (!(o instanceof List))
            return false;

        Iterator<V> e1 = iterator();
        Iterator<?> e2 = ((List<?>) o).iterator();
        while (e1.hasNext() && e2.hasNext()) {
            V o1 = e1.next();
            Object o2 = e2.next();
            if (!(o1==null ? o2==null : o1.equals(o2)))
                return false;
        }
        return !(e1.hasNext() || e2.hasNext());
    }

    @Override
    @SuppressWarnings("AvoidInlineConditionals")
    public int hashCode() {
        int hashCode = 1;
        for (V e : this) {
            hashCode = 31*hashCode + (e==null ? 0 : e.hashCode());
        }
        return hashCode;
    }

    @Override
    public RFuture<Integer> addAfterAsync(V elementToFind, V element) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.LINSERT_INT, getRawName(), "AFTER", encode(elementToFind), encode(element));
    }

    @Override
    public RFuture<Integer> addBeforeAsync(V elementToFind, V element) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.LINSERT_INT, getRawName(), "BEFORE", encode(elementToFind), encode(element));
    }

    @Override
    public int addAfter(V elementToFind, V element) {
        return get(addAfterAsync(elementToFind, element));
    }

    @Override
    public int addBefore(V elementToFind, V element) {
        return get(addBeforeAsync(elementToFind, element));
    }

    @Override
    public List<V> readSort(SortOrder order) {
        return get(readSortAsync(order));
    }
    
    @Override
    public RFuture<List<V>> readSortAsync(SortOrder order) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_LIST, getRawName(), order);
    }

    @Override
    public List<V> readSort(SortOrder order, int offset, int count) {
        return get(readSortAsync(order, offset, count));
    }
    
    @Override
    public RFuture<List<V>> readSortAsync(SortOrder order, int offset, int count) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_LIST, getRawName(), "LIMIT", offset, count, order);
    }

    @Override
    public List<V> readSort(String byPattern, SortOrder order) {
        return get(readSortAsync(byPattern, order));
    }
    
    @Override
    public RFuture<List<V>> readSortAsync(String byPattern, SortOrder order) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_LIST, getRawName(), "BY", byPattern, order);
    }
    
    @Override
    public List<V> readSort(String byPattern, SortOrder order, int offset, int count) {
        return get(readSortAsync(byPattern, order, offset, count));
    }
    
    @Override
    public RFuture<List<V>> readSortAsync(String byPattern, SortOrder order, int offset, int count) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_LIST, getRawName(), "BY", byPattern, "LIMIT", offset, count, order);
    }

    @Override
    public <T> Collection<T> readSort(String byPattern, List<String> getPatterns, SortOrder order) {
        return (Collection<T>) get(readSortAsync(byPattern, getPatterns, order));
    }
    
    @Override
    public <T> RFuture<Collection<T>> readSortAsync(String byPattern, List<String> getPatterns, SortOrder order) {
        return readSortAsync(byPattern, getPatterns, order, -1, -1);
    }

    @Override
    public <T> Collection<T> readSort(String byPattern, List<String> getPatterns, SortOrder order, int offset, int count) {
        return (Collection<T>) get(readSortAsync(byPattern, getPatterns, order, offset, count));
    }

    @Override
    public <T> RFuture<Collection<T>> readSortAsync(String byPattern, List<String> getPatterns, SortOrder order, int offset, int count) {
        return readSortAsync(byPattern, getPatterns, order, offset, count, false);
    }

    @Override
    public List<V> readSortAlpha(SortOrder order) {
        return get(readSortAlphaAsync(order));
    }

    @Override
    public RFuture<List<V>> readSortAlphaAsync(SortOrder order) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_LIST, getRawName(), "ALPHA", order);
    }

    @Override
    public List<V> readSortAlpha(SortOrder order, int offset, int count) {
        return get(readSortAlphaAsync(order, offset, count));
    }

    @Override
    public RFuture<List<V>> readSortAlphaAsync(SortOrder order, int offset, int count) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_LIST, getRawName(), "LIMIT", offset, count, "ALPHA", order);
    }

    @Override
    public List<V> readSortAlpha(String byPattern, SortOrder order) {
        return get(readSortAlphaAsync(byPattern, order));
    }

    @Override
    public RFuture<List<V>> readSortAlphaAsync(String byPattern, SortOrder order) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_LIST, getRawName(), "BY", byPattern, "ALPHA", order);
    }

    @Override
    public List<V> readSortAlpha(String byPattern, SortOrder order, int offset, int count) {
        return get(readSortAlphaAsync(byPattern, order, offset, count));
    }

    @Override
    public RFuture<List<V>> readSortAlphaAsync(String byPattern, SortOrder order, int offset, int count) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_LIST, getRawName(), "BY", byPattern, "LIMIT", offset, count, "ALPHA", order);
    }

    @Override
    public <T> Collection<T> readSortAlpha(String byPattern, List<String> getPatterns, SortOrder order) {
        return (Collection<T>) get(readSortAlphaAsync(byPattern, getPatterns, order));
    }

    @Override
    public <T> RFuture<Collection<T>> readSortAlphaAsync(String byPattern, List<String> getPatterns, SortOrder order) {
        return readSortAlphaAsync(byPattern, getPatterns, order, -1, -1);
    }

    @Override
    public <T> Collection<T> readSortAlpha(String byPattern, List<String> getPatterns, SortOrder order, int offset, int count) {
        return (Collection<T>) get(readSortAlphaAsync(byPattern, getPatterns, order, offset, count));
    }

    @Override
    public <T> RFuture<Collection<T>> readSortAlphaAsync(String byPattern, List<String> getPatterns, SortOrder order, int offset, int count) {
        return readSortAsync(byPattern, getPatterns, order, offset, count, true);
    }

    @Override
    public int sortTo(String destName, SortOrder order) {
        return get(sortToAsync(destName, order));
    }
    
    @Override
    public RFuture<Integer> sortToAsync(String destName, SortOrder order) {
        return sortToAsync(destName, null, Collections.<String>emptyList(), order, -1, -1);
    }
    
    @Override
    public int sortTo(String destName, SortOrder order, int offset, int count) {
        return get(sortToAsync(destName, order, offset, count));
    }
    
    @Override
    public RFuture<Integer> sortToAsync(String destName, SortOrder order, int offset, int count) {
        return sortToAsync(destName, null, Collections.<String>emptyList(), order, offset, count);
    }

    @Override
    public int sortTo(String destName, String byPattern, SortOrder order, int offset, int count) {
        return get(sortToAsync(destName, byPattern, order, offset, count));
    }
    
    @Override
    public int sortTo(String destName, String byPattern, SortOrder order) {
        return get(sortToAsync(destName, byPattern, order));
    }

    @Override
    public RFuture<Integer> sortToAsync(String destName, String byPattern, SortOrder order) {
        return sortToAsync(destName, byPattern, Collections.<String>emptyList(), order, -1, -1);
    }

    @Override
    public RFuture<Integer> sortToAsync(String destName, String byPattern, SortOrder order, int offset, int count) {
        return sortToAsync(destName, byPattern, Collections.<String>emptyList(), order, offset, count);
    }

    @Override
    public int sortTo(String destName, String byPattern, List<String> getPatterns, SortOrder order) {
        return get(sortToAsync(destName, byPattern, getPatterns, order));
    }
    
    @Override
    public RFuture<Integer> sortToAsync(String destName, String byPattern, List<String> getPatterns, SortOrder order) {
        return sortToAsync(destName, byPattern, getPatterns, order, -1, -1);
    }
    
    @Override
    public int sortTo(String destName, String byPattern, List<String> getPatterns, SortOrder order, int offset, int count) {
        return get(sortToAsync(destName, byPattern, getPatterns, order, offset, count));
    }

    @Override
    public RFuture<Integer> sortToAsync(String destName, String byPattern, List<String> getPatterns, SortOrder order, int offset, int count) {
        List<Object> params = new ArrayList<Object>();
        params.add(getRawName());
        if (byPattern != null) {
            params.add("BY");
            params.add(byPattern);
        }
        if (offset != -1 && count != -1) {
            params.add("LIMIT");
        }
        if (offset != -1) {
            params.add(offset);
        }
        if (count != -1) {
            params.add(count);
        }
        for (String pattern : getPatterns) {
            params.add("GET");
            params.add(pattern);
        }
        params.add(order);
        params.add("STORE");
        params.add(destName);
        
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.SORT_TO, params.toArray());
    }

    private <T> RFuture<Collection<T>> readSortAsync(String byPattern, List<String> getPatterns, SortOrder order, int offset, int count, boolean alpha) {
        List<Object> params = new ArrayList<Object>();
        params.add(getRawName());
        if (byPattern != null) {
            params.add("BY");
            params.add(byPattern);
        }
        if (offset != -1 && count != -1) {
            params.add("LIMIT");
        }
        if (offset != -1) {
            params.add(offset);
        }
        if (count != -1) {
            params.add(count);
        }
        if (getPatterns != null) {
            for (String pattern : getPatterns) {
                params.add("GET");
                params.add(pattern);
            }
        }
        if (alpha) {
            params.add("ALPHA");
        }
        if (order != null) {
            params.add(order);
        }

        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_LIST, params.toArray());
    }

    @Override
    public RFuture<List<V>> rangeAsync(int toIndex) {
        return rangeAsync(0, toIndex);
    }

    @Override
    public RFuture<List<V>> rangeAsync(int fromIndex, int toIndex) {
        return commandExecutor.readAsync(getRawName(), codec, LRANGE, getRawName(), fromIndex, toIndex);
    }

    @Override
    public List<V> range(int toIndex) {
        return get(rangeAsync(toIndex));
    }

    @Override
    public List<V> range(int fromIndex, int toIndex) {
        return get(rangeAsync(fromIndex, toIndex));
    }

    @Override
    public int addListener(ObjectListener listener, Integer previous) {
        if (listener instanceof ListAddListener) {
            previous = addListener("__keyevent@*:rpush", (ListAddListener) listener, ListAddListener::onListAdd, previous);
        }
        if (listener instanceof ListRemoveListener) {
            previous = addListener("__keyevent@*:lrem", (ListRemoveListener) listener, ListRemoveListener::onListRemove, previous);
        }
        if (listener instanceof ListTrimListener) {
            previous = addListener("__keyevent@*:ltrim", (ListTrimListener) listener, ListTrimListener::onListTrim, previous);
        }
        if (listener instanceof ListSetListener) {
            previous = addListener("__keyevent@*:lset", (ListSetListener) listener, ListSetListener::onListSet, previous);
        }
        if (listener instanceof ListInsertListener) {
            previous = addListener("__keyevent@*:linsert", (ListInsertListener) listener, ListInsertListener::onListInsert, previous);
        }
        return super.addListener(listener, previous);
    }

    @Override
    public RFuture<Integer> addListenerAsync(ObjectListener listener) {
        if (listener instanceof ListAddListener) {
            return addListenerAsync("__keyevent@*:rpush", (ListAddListener) listener, ListAddListener::onListAdd);
        }
        if (listener instanceof ListRemoveListener) {
            return addListenerAsync("__keyevent@*:lrem", (ListRemoveListener) listener, ListRemoveListener::onListRemove);
        }
        if (listener instanceof ListTrimListener) {
            return addListenerAsync("__keyevent@*:ltrim", (ListTrimListener) listener, ListTrimListener::onListTrim);
        }
        if (listener instanceof ListSetListener) {
            return addListenerAsync("__keyevent@*:lset", (ListSetListener) listener, ListSetListener::onListSet);
        }
        if (listener instanceof ListInsertListener) {
            return addListenerAsync("__keyevent@*:linsert", (ListInsertListener) listener, ListInsertListener::onListInsert);
        }
        return super.addListenerAsync(listener);
    }

    @Override
    public void removeListener(int listenerId) {
        RPatternTopic addTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:rpush");
        addTopic.removeListener(listenerId);

        RPatternTopic remTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:lrem");
        remTopic.removeListener(listenerId);

        RPatternTopic trimTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:ltrim");
        trimTopic.removeListener(listenerId);

        RPatternTopic setTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:lset");
        setTopic.removeListener(listenerId);

        RPatternTopic insertTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:linsert");
        insertTopic.removeListener(listenerId);

        super.removeListener(listenerId);
    }

    @Override
    public RFuture<Void> removeListenerAsync(int listenerId) {
        RPromise<Void> result = new RedissonPromise<>();
        CountableListener<Void> listener = new CountableListener<>(result, null, 5);

        RPatternTopic addTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:rpush");
        addTopic.removeListenerAsync(listenerId).onComplete(listener);

        RPatternTopic remTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:lrem");
        remTopic.removeListenerAsync(listenerId).onComplete(listener);

        RPatternTopic trimTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:ltrim");
        trimTopic.removeListenerAsync(listenerId).onComplete(listener);

        RPatternTopic setTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:lset");
        setTopic.removeListenerAsync(listenerId).onComplete(listener);

        RPatternTopic insertTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:linsert");
        insertTopic.removeListenerAsync(listenerId).onComplete(listener);

        removeListenersAsync(listenerId, listener);

        return result;
    }

    @Override
    public boolean removeIf(Predicate<? super V> filter) {
        throw new UnsupportedOperationException();
    }
}
