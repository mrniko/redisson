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
import org.redisson.api.listener.ScoredSortedSetAddListener;
import org.redisson.api.mapreduce.RCollectionMapReduce;
import org.redisson.client.RedisClient;
import org.redisson.client.codec.*;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.ScoredEntry;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.iterator.RedissonBaseIterator;
import org.redisson.mapreduce.RedissonCollectionMapReduce;
import org.redisson.misc.CountableListener;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;

import java.math.BigDecimal;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * 
 * @author Nikita Koksharov
 *
 * @param <V> value type
 */
public class RedissonScoredSortedSet<V> extends RedissonExpirable implements RScoredSortedSet<V> {

    private RedissonClient redisson;
    
    public RedissonScoredSortedSet(CommandAsyncExecutor commandExecutor, String name, RedissonClient redisson) {
        super(commandExecutor, name);
        this.redisson = redisson;
    }

    public RedissonScoredSortedSet(Codec codec, CommandAsyncExecutor commandExecutor, String name, RedissonClient redisson) {
        super(codec, commandExecutor, name);
        this.redisson = redisson;
    }

    @Override
    public <KOut, VOut> RCollectionMapReduce<V, KOut, VOut> mapReduce() {
        return new RedissonCollectionMapReduce<V, KOut, VOut>(this, redisson, commandExecutor);
    }

    @Override
    public Collection<V> readAll() {
        return get(readAllAsync());
    }
    
    @Override
    public RFuture<Collection<V>> readAllAsync() {
        return valueRangeAsync(0, -1);
    }
    
    @Override
    public V pollFirst() {
        return get(pollFirstAsync());
    }

    @Override
    public V pollLast() {
        return get(pollLastAsync());
    }

    @Override
    public Collection<V> pollFirst(int count) {
        return get(pollFirstAsync(count));
    }
    
    @Override
    public Collection<V> pollLast(int count) {
        return get(pollLastAsync(count));
    }
    
    @Override
    public RFuture<Collection<V>> pollFirstAsync(int count) {
        if (count <= 0) {
            return RedissonPromise.<Collection<V>>newSucceededFuture(Collections.<V>emptyList());
        }

        return poll(0, count-1, RedisCommands.EVAL_LIST);
    }
    
    @Override
    public RFuture<Collection<V>> pollLastAsync(int count) {
        if (count <= 0) {
            return RedissonPromise.<Collection<V>>newSucceededFuture(Collections.<V>emptyList());
        }
        return poll(-count, -1, RedisCommands.EVAL_LIST);
    }
    
    @Override
    public RFuture<V> pollFirstAsync() {
        return poll(0, 0, RedisCommands.EVAL_FIRST_LIST);
    }

    @Override
    public RFuture<V> pollLastAsync() {
        return poll(-1, -1, RedisCommands.EVAL_FIRST_LIST);
    }

    private <T> RFuture<T> poll(int from, int to, RedisCommand<?> command) {
        return commandExecutor.evalWriteAsync(getRawName(), codec, command,
                "local v = redis.call('zrange', KEYS[1], ARGV[1], ARGV[2]); "
                + "if #v > 0 then "
                    + "redis.call('zremrangebyrank', KEYS[1], ARGV[1], ARGV[2]); "
                    + "return v; "
                + "end "
                + "return v;",
                Collections.<Object>singletonList(getRawName()), from, to);
    }

    @Override
    public V pollFirst(long timeout, TimeUnit unit) {
        return get(pollFirstAsync(timeout, unit));
    }
    
    @Override
    public RFuture<V> pollFirstAsync(long timeout, TimeUnit unit) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.BZPOPMIN_VALUE, getRawName(), toSeconds(timeout, unit));
    }
    
    @Override
    public V pollFirstFromAny(long timeout, TimeUnit unit, String... queueNames) {
        return get(pollFirstFromAnyAsync(timeout, unit, queueNames));
    }

    @Override
    public RFuture<V> pollFirstFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
        return commandExecutor.pollFromAnyAsync(getRawName(), codec, RedisCommands.BZPOPMIN_VALUE, toSeconds(timeout, unit), queueNames);
    }

    @Override
    public V pollLastFromAny(long timeout, TimeUnit unit, String... queueNames) {
        return get(pollLastFromAnyAsync(timeout, unit, queueNames));
    }

    @Override
    public RFuture<V> pollLastFromAnyAsync(long timeout, TimeUnit unit, String... queueNames) {
        return commandExecutor.pollFromAnyAsync(getRawName(), codec, RedisCommands.BZPOPMAX_VALUE, toSeconds(timeout, unit), queueNames);
    }
    
    @Override
    public V pollLast(long timeout, TimeUnit unit) {
        return get(pollLastAsync(timeout, unit));
    }
    
    @Override
    public RFuture<V> pollLastAsync(long timeout, TimeUnit unit) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.BZPOPMAX_VALUE, getRawName(), toSeconds(timeout, unit));
    }

    @Override
    public V random() {
        return get(randomAsync());
    }

    @Override
    public Collection<V> random(int count) {
        return get(randomAsync(count));
    }

    @Override
    public RFuture<V> randomAsync() {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZRANDMEMBER_SINGLE, getRawName());
    }

    @Override
    public RFuture<Collection<V>> randomAsync(int count) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZRANDMEMBER, getRawName(), count);
    }

    @Override
    public Map<V, Double> randomEntries(int count) {
        return get(randomEntriesAsync(count));
    }

    @Override
    public RFuture<Map<V, Double>> randomEntriesAsync(int count) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZRANDMEMBER_ENTRIES, getRawName(), count, "WITHSCORES");
    }

    @Override
    public boolean add(double score, V object) {
        return get(addAsync(score, object));
    }

    @Override
    public Integer addAndGetRank(double score, V object) {
        return get(addAndGetRankAsync(score, object));
    }

    @Override
    public RFuture<Integer> addAndGetRankAsync(double score, V object) {
        return commandExecutor.evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_INTEGER,
                "redis.call('zadd', KEYS[1], ARGV[1], ARGV[2]);" +
                "return redis.call('zrank', KEYS[1], ARGV[2]); ",
                Collections.<Object>singletonList(getRawName()), new BigDecimal(score).toPlainString(), encode(object));
    }

    @Override
    public Integer addAndGetRevRank(double score, V object) {
        return get(addAndGetRevRankAsync(score, object));
    }

    @Override
    public List<Integer> addAndGetRevRank(Map<? extends V, Double> map) {
        return get(addAndGetRevRankAsync(map));
    }

    @Override
    public RFuture<Integer> addAndGetRevRankAsync(double score, V object) {
        return commandExecutor.evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_INTEGER,
                "redis.call('zadd', KEYS[1], ARGV[1], ARGV[2]);" +
                "return redis.call('zrevrank', KEYS[1], ARGV[2]); ",
                Collections.<Object>singletonList(getRawName()), new BigDecimal(score).toPlainString(), encode(object));
    }

    @Override
    public RFuture<List<Integer>> addAndGetRevRankAsync(Map<? extends V, Double> map) {
        final List<Object> params = new ArrayList<Object>(map.size() * 2);
        for (java.util.Map.Entry<? extends V, Double> t : map.entrySet()) {
            if (t.getKey() == null) {
                throw new NullPointerException("map key can't be null");
            }
            if (t.getValue() == null) {
                throw new NullPointerException("map value can't be null");
            }
            params.add(encode(t.getKey()));
            params.add(BigDecimal.valueOf(t.getValue()).toPlainString());
        }

        return commandExecutor.evalWriteAsync((String) null, IntegerCodec.INSTANCE, RedisCommands.EVAL_INT_LIST,
                    "local r = {} " +
                    "for i, v in ipairs(ARGV) do " +
                        "if i % 2 == 0 then " +
                            "redis.call('zadd', KEYS[1], ARGV[i], ARGV[i-1]); " +
                        "end; " +
                    "end;" +
                    "for i, v in ipairs(ARGV) do " +
                        "if i % 2 == 0 then " +
                            "r[#r+1] = redis.call('zrevrank', KEYS[1], ARGV[i-1]); " +
                        "end; " +
                    "end;" +
                    "return r;",
                Collections.singletonList(getRawName()), params.toArray());
    }

    @Override
    public boolean tryAdd(double score, V object) {
        return get(tryAddAsync(score, object));
    }

    @Override
    public boolean addIfExists(double score, V object) {
        return get(addIfExistsAsync(score, object));
    }

    @Override
    public RFuture<Boolean> addIfExistsAsync(double score, V object) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZADD_BOOL, getRawName(), "XX", BigDecimal.valueOf(score).toPlainString(), encode(object));
    }

    @Override
    public boolean addIfLess(double score, V object) {
        return get(addIfLessAsync(score, object));
    }

    @Override
    public boolean addIfGreater(double score, V object) {
        return get(addIfGreaterAsync(score, object));
    }

    @Override
    public RFuture<Boolean> addIfLessAsync(double score, V object) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZADD_BOOL, getRawName(), "LT", BigDecimal.valueOf(score).toPlainString(), encode(object));
    }

    @Override
    public RFuture<Boolean> addIfGreaterAsync(double score, V object) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZADD_BOOL, getRawName(), "GT", BigDecimal.valueOf(score).toPlainString(), encode(object));
    }

    @Override
    public V first() {
        return get(firstAsync());
    }

    @Override
    public RFuture<V> firstAsync() {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGE_SINGLE, getRawName(), 0, 0);
    }

    @Override
    public V last() {
        return get(lastAsync());
    }

    @Override
    public RFuture<V> lastAsync() {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGE_SINGLE, getRawName(), -1, -1);
    }
    
    @Override
    public Double firstScore() {
        return get(firstScoreAsync());
    }

    @Override
    public RFuture<Double> firstScoreAsync() {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGE_SINGLE_SCORE, getRawName(), 0, 0, "WITHSCORES");
    }
    
    @Override
    public Double lastScore() {
        return get(lastScoreAsync());
    }

    @Override
    public RFuture<Double> lastScoreAsync() {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGE_SINGLE_SCORE, getRawName(), -1, -1, "WITHSCORES");
    }


    @Override
    public RFuture<Boolean> addAsync(double score, V object) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZADD_BOOL, getRawName(), BigDecimal.valueOf(score).toPlainString(), encode(object));
    }

    @Override
    public int addAll(Map<V, Double> objects) {
        return get(addAllAsync(objects));
    }

    @Override
    public RFuture<Integer> addAllAsync(Map<V, Double> objects) {
        if (objects.isEmpty()) {
            return RedissonPromise.newSucceededFuture(0);
        }
        List<Object> params = new ArrayList<Object>(objects.size()*2+1);
        params.add(getRawName());
        for (Entry<V, Double> entry : objects.entrySet()) {
            params.add(BigDecimal.valueOf(entry.getValue()).toPlainString());
            params.add(encode(entry.getKey()));
        }

        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZADD_INT, params.toArray());
    }

    @Override
    public RFuture<Boolean> tryAddAsync(double score, V object) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZADD_BOOL, getRawName(), "NX", BigDecimal.valueOf(score).toPlainString(), encode(object));
    }

    @Override
    public boolean remove(Object object) {
        return get(removeAsync(object));
    }

    @Override
    public int removeRangeByRank(int startIndex, int endIndex) {
        return get(removeRangeByRankAsync(startIndex, endIndex));
    }

    @Override
    public RFuture<Integer> removeRangeByRankAsync(int startIndex, int endIndex) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZREMRANGEBYRANK, getRawName(), startIndex, endIndex);
    }

    @Override
    public int removeRangeByScore(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        return get(removeRangeByScoreAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
    }

    @Override
    public RFuture<Integer> removeRangeByScoreAsync(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZREMRANGEBYSCORE, getRawName(), startValue, endValue);
    }

    private String value(double score, boolean inclusive) {
        StringBuilder element = new StringBuilder();
        if (!inclusive) {
            element.append("(");
        }
        if (Double.isInfinite(score)) {
            if (score > 0) {
                element.append("+inf");
            } else {
                element.append("-inf");
            }
        } else {
            element.append(BigDecimal.valueOf(score).toPlainString());
        }
        return element.toString();
    }

    @Override
    public void clear() {
        delete();
    }

    @Override
    public RFuture<Boolean> removeAsync(Object object) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZREM, getRawName(), encode(object));
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public int size() {
        return get(sizeAsync());
    }

    @Override
    public RFuture<Integer> sizeAsync() {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZCARD_INT, getRawName());
    }

    @Override
    public boolean contains(Object object) {
        return get(containsAsync(object));
    }

    @Override
    public RFuture<Boolean> containsAsync(Object o) {
        return commandExecutor.readAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.ZSCORE_CONTAINS, getRawName(), encode(o));
    }

    @Override
    public Double getScore(V o) {
        return get(getScoreAsync(o));
    }

    @Override
    public List<Double> getScore(List<V> keys) {
        return get(getScoreAsync(keys));
    }

    @Override
    public RFuture<Double> getScoreAsync(V o) {
        return commandExecutor.readAsync(getRawName(), StringCodec.INSTANCE, RedisCommands.ZSCORE, getRawName(), encode(o));
    }

    @Override
    public RFuture<List<Double>> getScoreAsync(Collection<V> elements) {
        return commandExecutor.evalReadAsync((String) null, DoubleCodec.INSTANCE, RedisCommands.EVAL_LIST,
                "local r = {} " +
                "for i, v in ipairs(ARGV) do " +
                    "r[#r+1] = redis.call('ZSCORE', KEYS[1], ARGV[i]); " +
                "end;" +
                "return r;",
                Collections.singletonList(getRawName()), encode(elements).toArray());
    }

    @Override
    public Integer rank(V o) {
        return get(rankAsync(o));
    }

    @Override
    public RFuture<Integer> rankAsync(V o) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANK_INT, getRawName(), encode(o));
    }

    private ScanResult<Object> scanIterator(RedisClient client, long startPos, String pattern, int count) {
        RFuture<ScanResult<Object>> f = scanIteratorAsync(client, startPos, pattern, count);
        return get(f);
    }
    
    public RFuture<ScanResult<Object>> scanIteratorAsync(RedisClient client, long startPos, String pattern, int count) {
        if (pattern == null) {
            RFuture<ScanResult<Object>> f = commandExecutor.readAsync(client, getRawName(), codec, RedisCommands.ZSCAN, getRawName(), startPos, "COUNT", count);
            return f;
        }
        RFuture<ScanResult<Object>> f = commandExecutor.readAsync(client, getRawName(), codec, RedisCommands.ZSCAN, getRawName(), startPos, "MATCH", pattern, "COUNT", count);
        return f;
    }

    @Override
    public Iterator<V> iterator() {
        return iterator(null, 10);
    }
    
    @Override
    public Iterator<V> iterator(String pattern) {
        return iterator(pattern, 10);
    }
    
    @Override
    public Iterator<V> iterator(int count) {
        return iterator(null, count);
    }

    @Override
    public Iterator<V> iterator(String pattern, int count) {
        return new RedissonBaseIterator<V>() {

            @Override
            protected ScanResult<Object> iterator(RedisClient client, long nextIterPos) {
                return scanIterator(client, nextIterPos, pattern, count);
            }

            @Override
            protected void remove(Object value) {
                RedissonScoredSortedSet.this.remove((V) value);
            }
            
        };
    }
    
    @Override
    public Object[] toArray() {
        List<Object> res = (List<Object>) get(valueRangeAsync(0, -1));
        return res.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        List<Object> res = (List<Object>) get(valueRangeAsync(0, -1));
        return res.toArray(a);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return get(containsAllAsync(c));
    }

    @Override
    public RFuture<Boolean> containsAllAsync(Collection<?> c) {
        if (c.isEmpty()) {
            return RedissonPromise.newSucceededFuture(true);
        }
        
        return commandExecutor.evalReadAsync(getRawName(), codec, RedisCommands.EVAL_BOOLEAN,
                            "for j = 1, #ARGV, 1 do "
                            + "local expireDateScore = redis.call('zscore', KEYS[1], ARGV[j]) "
                            + "if expireDateScore == false then "
                                + "return 0;"
                            + "end; "
                        + "end; "
                       + "return 1; ",
                Collections.<Object>singletonList(getRawName()), encode(c).toArray());
    }

    @Override
    public RFuture<Boolean> removeAllAsync(Collection<?> c) {
        if (c.isEmpty()) {
            return RedissonPromise.newSucceededFuture(false);
        }
        
        List<Object> params = new ArrayList<Object>(c.size()+1);
        params.add(getRawName());
        encode(params, c);

        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZREM, params.toArray());
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
        
        List<Object> params = new ArrayList<Object>(c.size()*2);
        for (Object object : c) {
            params.add(0);
            params.add(encode((V) object));
        }
        
        return commandExecutor.evalWriteAsync(getRawName(), codec, RedisCommands.EVAL_BOOLEAN,
                "redis.call('zadd', KEYS[2], unpack(ARGV)); "
                 + "local prevSize = redis.call('zcard', KEYS[1]); "
                 + "local size = redis.call('zinterstore', KEYS[1], 2, KEYS[1], KEYS[2], 'aggregate', 'sum');"
                 + "redis.call('del', KEYS[2]); "
                 + "return size ~= prevSize and 1 or 0; ",
             Arrays.<Object>asList(getRawName(), "redisson_temp__{" + getRawName() + "}"), params.toArray());
    }

    @Override
    public Double addScore(V object, Number value) {
        return get(addScoreAsync(object, value));
    }

    @Override
    public RFuture<Double> addScoreAsync(V object, Number value) {
        return commandExecutor.writeAsync(getRawName(), DoubleCodec.INSTANCE, RedisCommands.ZINCRBY,
                                   getRawName(), new BigDecimal(value.toString()).toPlainString(), encode(object));
    }
    
    @Override
    public Integer addScoreAndGetRank(V object, Number value) {
        return get(addScoreAndGetRankAsync(object, value));
    }
    
    @Override
    public RFuture<Integer> addScoreAndGetRankAsync(V object, Number value) {
        return commandExecutor.evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_INTEGER,
                "redis.call('zincrby', KEYS[1], ARGV[1], ARGV[2]); "
               +"return redis.call('zrank', KEYS[1], ARGV[2]); ",
                Collections.<Object>singletonList(getRawName()), new BigDecimal(value.toString()).toPlainString(), encode(object));
    }

    @Override
    public Integer addScoreAndGetRevRank(V object, Number value) {
        return get(addScoreAndGetRevRankAsync(object, value));
    }
    
    @Override
    public RFuture<Integer> addScoreAndGetRevRankAsync(V object, Number value) {
        return commandExecutor.evalWriteAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_INTEGER,
                "redis.call('zincrby', KEYS[1], ARGV[1], ARGV[2]); "
               +"return redis.call('zrevrank', KEYS[1], ARGV[2]); ",
                Collections.<Object>singletonList(getRawName()), new BigDecimal(value.toString()).toPlainString(), encode(object));
    }

    @Override
    public Collection<V> valueRange(int startIndex, int endIndex) {
        return get(valueRangeAsync(startIndex, endIndex));
    }

    @Override
    public RFuture<Collection<V>> valueRangeAsync(int startIndex, int endIndex) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGE, getRawName(), startIndex, endIndex);
    }

    @Override
    public Collection<V> valueRangeReversed(int startIndex, int endIndex) {
        return get(valueRangeReversedAsync(startIndex, endIndex));
    }
    
    @Override
    public RFuture<Collection<V>> valueRangeReversedAsync(int startIndex, int endIndex) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZREVRANGE, getRawName(), startIndex, endIndex);
    }
    
    @Override
    public Collection<ScoredEntry<V>> entryRange(int startIndex, int endIndex) {
        return get(entryRangeAsync(startIndex, endIndex));
    }

    @Override
    public RFuture<Collection<ScoredEntry<V>>> entryRangeAsync(int startIndex, int endIndex) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGE_ENTRY, getRawName(), startIndex, endIndex, "WITHSCORES");
    }

    @Override
    public Collection<ScoredEntry<V>> entryRangeReversed(int startIndex, int endIndex) {
        return get(entryRangeReversedAsync(startIndex, endIndex));
    }
    
    @Override
    public RFuture<Collection<ScoredEntry<V>>> entryRangeReversedAsync(int startIndex, int endIndex) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZREVRANGE_ENTRY, getRawName(), startIndex, endIndex, "WITHSCORES");
    }

    @Override
    public Collection<V> valueRange(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        return get(valueRangeAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
    }

    @Override
    public RFuture<Collection<V>> valueRangeAsync(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGEBYSCORE_LIST, getRawName(), startValue, endValue);
    }

    @Override
    public Collection<V> valueRangeReversed(double startScore, boolean startScoreInclusive, double endScore,
            boolean endScoreInclusive) {
        return get(valueRangeReversedAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
    }

    @Override
    public RFuture<Collection<V>> valueRangeReversedAsync(double startScore, boolean startScoreInclusive, double endScore,
            boolean endScoreInclusive) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZREVRANGEBYSCORE, getRawName(), endValue, startValue);
    }


    @Override
    public Collection<ScoredEntry<V>> entryRange(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        return get(entryRangeAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
    }

    @Override
    public RFuture<Collection<ScoredEntry<V>>> entryRangeAsync(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGEBYSCORE_ENTRY, getRawName(), startValue, endValue, "WITHSCORES");
    }

    @Override
    public Collection<V> valueRange(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        return get(valueRangeAsync(startScore, startScoreInclusive, endScore, endScoreInclusive, offset, count));
    }

    @Override
    public RFuture<Collection<V>> valueRangeAsync(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGEBYSCORE_LIST, getRawName(), startValue, endValue, "LIMIT", offset, count);
    }

    @Override
    public Collection<V> valueRangeReversed(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        return get(valueRangeReversedAsync(startScore, startScoreInclusive, endScore, endScoreInclusive, offset, count));
    }

    @Override
    public RFuture<Collection<V>> valueRangeReversedAsync(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZREVRANGEBYSCORE, getRawName(), endValue, startValue, "LIMIT", offset, count);
    }

    @Override
    public Collection<ScoredEntry<V>> entryRange(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        return get(entryRangeAsync(startScore, startScoreInclusive, endScore, endScoreInclusive, offset, count));
    }

    @Override
    public Collection<ScoredEntry<V>> entryRangeReversed(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        return get(entryRangeReversedAsync(startScore, startScoreInclusive, endScore, endScoreInclusive, offset, count));
    }

    @Override
    public RFuture<Collection<ScoredEntry<V>>> entryRangeAsync(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZRANGEBYSCORE_ENTRY, getRawName(), startValue, endValue, "WITHSCORES", "LIMIT", offset, count);
    }

    @Override
    public Collection<ScoredEntry<V>> entryRangeReversed(double startScore, boolean startScoreInclusive,
            double endScore, boolean endScoreInclusive) {
        return get(entryRangeReversedAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
    }
    
    @Override
    public RFuture<Collection<ScoredEntry<V>>> entryRangeReversedAsync(double startScore, boolean startScoreInclusive,
            double endScore, boolean endScoreInclusive) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZREVRANGEBYSCORE_ENTRY, getRawName(), endValue, startValue, "WITHSCORES");
    }
    
    @Override
    public RFuture<Collection<ScoredEntry<V>>> entryRangeReversedAsync(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZREVRANGEBYSCORE_ENTRY, getRawName(), endValue, startValue, "WITHSCORES", "LIMIT", offset, count);
    }

    @Override
    public RFuture<Integer> revRankAsync(V o) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZREVRANK_INT, getRawName(), encode(o));
    }

    @Override
    public Integer revRank(V o) {
        return get(revRankAsync(o));
    }

    @Override
    public RFuture<List<Integer>> revRankAsync(Collection<V> elements) {
        return commandExecutor.evalReadAsync((String) null, IntegerCodec.INSTANCE, RedisCommands.EVAL_INT_LIST,
                        "local r = {} " +
                        "for i, v in ipairs(ARGV) do " +
                            "r[#r+1] = redis.call('zrevrank', KEYS[1], ARGV[i]); " +
                        "end;" +
                        "return r;",
                Collections.singletonList(getRawName()), encode(elements).toArray());
    }

    @Override
    public List<Integer> revRank(Collection<V> elements) {
        return get(revRankAsync(elements));
    }

    @Override
    public int count(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        return get(countAsync(startScore, startScoreInclusive, endScore, endScoreInclusive));
    }
    
    @Override
    public RFuture<Integer> countAsync(double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZCOUNT, getRawName(), startValue, endValue);
    }
    
    @Override
    public int intersection(String... names) {
        return get(intersectionAsync(names));        
    }

    @Override
    public RFuture<Integer> intersectionAsync(String... names) {
        return intersectionAsync(Aggregate.SUM, names);
    }
    
    @Override
    public int intersection(Aggregate aggregate, String... names) {
        return get(intersectionAsync(aggregate, names));        
    }
    
    @Override
    public RFuture<Integer> intersectionAsync(Aggregate aggregate, String... names) {
        List<Object> args = new ArrayList<Object>(names.length + 4);
        args.add(getRawName());
        args.add(names.length);
        args.addAll(Arrays.asList(names));
        args.add("AGGREGATE");
        args.add(aggregate.name());
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.ZINTERSTORE_INT, args.toArray());
    }

    @Override
    public int intersection(Map<String, Double> nameWithWeight) {
        return get(intersectionAsync(nameWithWeight));        
    }
    
    @Override
    public RFuture<Integer> intersectionAsync(Map<String, Double> nameWithWeight) {
        return intersectionAsync(Aggregate.SUM, nameWithWeight);
    }

    @Override
    public int intersection(Aggregate aggregate, Map<String, Double> nameWithWeight) {
        return get(intersectionAsync(aggregate, nameWithWeight));        
    }

    @Override
    public RFuture<Integer> intersectionAsync(Aggregate aggregate, Map<String, Double> nameWithWeight) {
        List<Object> args = new ArrayList<Object>(nameWithWeight.size()*2 + 5);
        args.add(getRawName());
        args.add(nameWithWeight.size());
        args.addAll(nameWithWeight.keySet());
        args.add("WEIGHTS");
        List<String> weights = new ArrayList<String>();
        for (Double weight : nameWithWeight.values()) {
            weights.add(BigDecimal.valueOf(weight).toPlainString());
        }
        args.addAll(weights);
        args.add("AGGREGATE");
        args.add(aggregate.name());
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.ZINTERSTORE_INT, args.toArray());
    }

    @Override
    public Collection<V> readIntersection(String... names) {
        return get(readIntersectionAsync(names));
    }

    @Override
    public RFuture<Collection<V>> readIntersectionAsync(String... names) {
        return readIntersectionAsync(Aggregate.SUM, names);
    }

    @Override
    public Collection<V> readIntersection(Aggregate aggregate, String... names) {
        return get(readIntersectionAsync(aggregate, names));
    }

    @Override
    public RFuture<Collection<V>> readIntersectionAsync(Aggregate aggregate, String... names) {
        List<Object> args = new ArrayList<Object>(names.length + 4);
        args.add(names.length + 1);
        args.add(getRawName());
        args.addAll(Arrays.asList(names));
        args.add("AGGREGATE");
        args.add(aggregate.name());
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZINTER, args.toArray());
    }

    @Override
    public Collection<V> readIntersection(Map<String, Double> nameWithWeight) {
        return get(readIntersectionAsync(nameWithWeight));
    }

    @Override
    public RFuture<Collection<V>> readIntersectionAsync(Map<String, Double> nameWithWeight) {
        return readIntersectionAsync(Aggregate.SUM, nameWithWeight);
    }

    @Override
    public Collection<V> readIntersection(Aggregate aggregate, Map<String, Double> nameWithWeight) {
        return get(readIntersectionAsync(aggregate, nameWithWeight));
    }

    @Override
    public RFuture<Collection<V>> readIntersectionAsync(Aggregate aggregate, Map<String, Double> nameWithWeight) {
        List<Object> args = new ArrayList<Object>(nameWithWeight.size()*2 + 5);
        args.add(nameWithWeight.size() + 1);
        args.add(getRawName());
        args.addAll(nameWithWeight.keySet());
        args.add("WEIGHTS");
        List<String> weights = new ArrayList<String>();
        for (Double weight : nameWithWeight.values()) {
            weights.add(BigDecimal.valueOf(weight).toPlainString());
        }
        args.addAll(weights);
        args.add("AGGREGATE");
        args.add(aggregate.name());
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZINTER, args.toArray());
    }

    @Override
    public int union(String... names) {
        return get(unionAsync(names));        
    }

    @Override
    public RFuture<Integer> unionAsync(String... names) {
        return unionAsync(Aggregate.SUM, names);
    }
    
    @Override
    public int union(Aggregate aggregate, String... names) {
        return get(unionAsync(aggregate, names));        
    }
    
    @Override
    public RFuture<Integer> unionAsync(Aggregate aggregate, String... names) {
        List<Object> args = new ArrayList<Object>(names.length + 4);
        args.add(getRawName());
        args.add(names.length);
        args.addAll(Arrays.asList(names));
        args.add("AGGREGATE");
        args.add(aggregate.name());
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.ZUNIONSTORE_INT, args.toArray());
    }

    @Override
    public int union(Map<String, Double> nameWithWeight) {
        return get(unionAsync(nameWithWeight));        
    }
    
    @Override
    public RFuture<Integer> unionAsync(Map<String, Double> nameWithWeight) {
        return unionAsync(Aggregate.SUM, nameWithWeight);
    }

    @Override
    public int union(Aggregate aggregate, Map<String, Double> nameWithWeight) {
        return get(unionAsync(aggregate, nameWithWeight));        
    }

    @Override
    public RFuture<Integer> unionAsync(Aggregate aggregate, Map<String, Double> nameWithWeight) {
        List<Object> args = new ArrayList<Object>(nameWithWeight.size()*2 + 5);
        args.add(getRawName());
        args.add(nameWithWeight.size());
        args.addAll(nameWithWeight.keySet());
        args.add("WEIGHTS");
        List<String> weights = new ArrayList<String>();
        for (Double weight : nameWithWeight.values()) {
            weights.add(BigDecimal.valueOf(weight).toPlainString());
        }
        args.addAll(weights);
        args.add("AGGREGATE");
        args.add(aggregate.name());
        return commandExecutor.writeAsync(getRawName(), LongCodec.INSTANCE, RedisCommands.ZUNIONSTORE_INT, args.toArray());
    }

    @Override
    public Collection<V> readUnion(String... names) {
        return get(readUnionAsync(names));
    }

    @Override
    public RFuture<Collection<V>> readUnionAsync(String... names) {
        return readUnionAsync(Aggregate.SUM, names);
    }

    @Override
    public Collection<V> readUnion(Aggregate aggregate, String... names) {
        return get(readUnionAsync(aggregate, names));
    }

    @Override
    public RFuture<Collection<V>> readUnionAsync(Aggregate aggregate, String... names) {
        List<Object> args = new ArrayList<>(names.length + 4);
        args.add(names.length + 1);
        args.add(getRawName());
        args.addAll(Arrays.asList(names));
        args.add("AGGREGATE");
        args.add(aggregate.name());
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZUNION, args.toArray());
    }

    @Override
    public Collection<V> readUnion(Map<String, Double> nameWithWeight) {
        return get(readUnionAsync(nameWithWeight));
    }

    @Override
    public RFuture<Collection<V>> readUnionAsync(Map<String, Double> nameWithWeight) {
        return readUnionAsync(Aggregate.SUM, nameWithWeight);
    }

    @Override
    public Collection<V> readUnion(Aggregate aggregate, Map<String, Double> nameWithWeight) {
        return get(readUnionAsync(aggregate, nameWithWeight));
    }

    @Override
    public RFuture<Collection<V>> readUnionAsync(Aggregate aggregate, Map<String, Double> nameWithWeight) {
        List<Object> args = new ArrayList<Object>(nameWithWeight.size()*2 + 5);
        args.add(nameWithWeight.size() + 1);
        args.add(getRawName());
        args.addAll(nameWithWeight.keySet());
        args.add("WEIGHTS");
        List<String> weights = new ArrayList<String>();
        for (Double weight : nameWithWeight.values()) {
            weights.add(BigDecimal.valueOf(weight).toPlainString());
        }
        args.addAll(weights);
        args.add("AGGREGATE");
        args.add(aggregate.name());
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZUNION, args.toArray());
    }

    @Override
    public Set<V> readSort(SortOrder order) {
        return get(readSortAsync(order));
    }
    
    @Override
    public RFuture<Set<V>> readSortAsync(SortOrder order) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_SET, getRawName(), order);
    }

    @Override
    public Set<V> readSort(SortOrder order, int offset, int count) {
        return get(readSortAsync(order, offset, count));
    }
    
    @Override
    public RFuture<Set<V>> readSortAsync(SortOrder order, int offset, int count) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_SET, getRawName(), "LIMIT", offset, count, order);
    }

    @Override
    public Set<V> readSort(String byPattern, SortOrder order) {
        return get(readSortAsync(byPattern, order));
    }
    
    @Override
    public RFuture<Set<V>> readSortAsync(String byPattern, SortOrder order) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_SET, getRawName(), "BY", byPattern, order);
    }
    
    @Override
    public Set<V> readSort(String byPattern, SortOrder order, int offset, int count) {
        return get(readSortAsync(byPattern, order, offset, count));
    }
    
    @Override
    public RFuture<Set<V>> readSortAsync(String byPattern, SortOrder order, int offset, int count) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_SET, getRawName(), "BY", byPattern, "LIMIT", offset, count, order);
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
    public Set<V> readSortAlpha(SortOrder order) {
        return get(readSortAlphaAsync(order));
    }

    @Override
    public Set<V> readSortAlpha(SortOrder order, int offset, int count) {
        return get(readSortAlphaAsync(order, offset, count));
    }

    @Override
    public Set<V> readSortAlpha(String byPattern, SortOrder order) {
        return get(readSortAlphaAsync(byPattern, order));
    }

    @Override
    public Set<V> readSortAlpha(String byPattern, SortOrder order, int offset, int count) {
        return get(readSortAlphaAsync(byPattern, order, offset, count));
    }

    @Override
    public <T> Collection<T> readSortAlpha(String byPattern, List<String> getPatterns, SortOrder order) {
        return (Collection<T>) get(readSortAlphaAsync(byPattern, getPatterns, order));
    }

    @Override
    public <T> Collection<T> readSortAlpha(String byPattern, List<String> getPatterns, SortOrder order, int offset, int count) {
        return (Collection<T>) get(readSortAlphaAsync(byPattern, getPatterns, order, offset, count));
    }

    @Override
    public RFuture<Set<V>> readSortAlphaAsync(SortOrder order) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_SET, getRawName(), "ALPHA", order);
    }

    @Override
    public RFuture<Set<V>> readSortAlphaAsync(SortOrder order, int offset, int count) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_SET, getRawName(), "LIMIT", offset, count, "ALPHA", order);
    }

    @Override
    public RFuture<Set<V>> readSortAlphaAsync(String byPattern, SortOrder order) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_SET, getRawName(), "BY", byPattern, "ALPHA", order);
    }

    @Override
    public RFuture<Set<V>> readSortAlphaAsync(String byPattern, SortOrder order, int offset, int count) {
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_SET, getRawName(), "BY", byPattern, "LIMIT", offset, count, "ALPHA", order);
    }

    @Override
    public <T> RFuture<Collection<T>> readSortAlphaAsync(String byPattern, List<String> getPatterns, SortOrder order) {
        return readSortAlphaAsync(byPattern, getPatterns, order, -1, -1);
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

        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.SORT_SET, params.toArray());
    }

    @Override
    public Collection<V> readDiff(String... names) {
        return get(readDiffAsync(names));
    }

    @Override
    public RFuture<Collection<V>> readDiffAsync(String... names) {
        List<Object> args = new ArrayList<Object>(names.length + 2);
        args.add(names.length + 1);
        args.add(getRawName());
        args.addAll(Arrays.asList(names));
        return commandExecutor.readAsync(getRawName(), codec, RedisCommands.ZDIFF, args.toArray());
    }

    @Override
    public int diff(String... names) {
        return get(diffAsync(names));
    }

    @Override
    public RFuture<Integer> diffAsync(String... names) {
        List<Object> args = new ArrayList<>(names.length + 2);
        args.add(getRawName());
        args.add(names.length);
        args.addAll(Arrays.asList(names));
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZDIFFSTORE_INT, args.toArray());
    }

    @Override
    public int rangeTo(String destName, int startIndex, int endIndex) {
        return get(rangeToAsync(destName, startIndex, endIndex));
    }

    @Override
    public int rangeTo(String destName, double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        return get(rangeToAsync(destName, startScore, startScoreInclusive, endScore, endScoreInclusive));
    }

    @Override
    public int rangeTo(String destName, double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        return get(rangeToAsync(destName, startScore, startScoreInclusive, endScore, endScoreInclusive, offset, count));
    }

    @Override
    public int revRangeTo(String destName, int startIndex, int endIndex) {
        return get(revRangeToAsync(destName, startIndex, endIndex));
    }

    @Override
    public int revRangeTo(String destName, double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        return get(revRangeToAsync(destName, startScore, startScoreInclusive, endScore, endScoreInclusive));
    }

    @Override
    public int revRangeTo(String destName, double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        return get(revRangeToAsync(destName, startScore, startScoreInclusive, endScore, endScoreInclusive, offset, count));
    }

    @Override
    public RFuture<Integer> revRangeToAsync(String destName, int startIndex, int endIndex) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZRANGESTORE, destName, getRawName(), startIndex, endIndex, "REV");
    }

    @Override
    public RFuture<Integer> revRangeToAsync(String destName, double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZRANGESTORE, destName, getRawName(), startValue, endValue, "BYSCORE", "REV");
    }

    @Override
    public RFuture<Integer> revRangeToAsync(String destName, double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZRANGESTORE, destName, getRawName(), startValue, endValue, "BYSCORE", "REV", "LIMIT", offset, count);
    }

    @Override
    public RFuture<Integer> rangeToAsync(String destName, int startIndex, int endIndex) {
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZRANGESTORE, destName, getRawName(), startIndex, endIndex);
    }

    @Override
    public RFuture<Integer> rangeToAsync(String destName, double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZRANGESTORE, destName, getRawName(), startValue, endValue, "BYSCORE");
    }

    @Override
    public RFuture<Integer> rangeToAsync(String destName, double startScore, boolean startScoreInclusive, double endScore, boolean endScoreInclusive, int offset, int count) {
        String startValue = value(startScore, startScoreInclusive);
        String endValue = value(endScore, endScoreInclusive);
        return commandExecutor.writeAsync(getRawName(), codec, RedisCommands.ZRANGESTORE, destName, getRawName(), startValue, endValue, "BYSCORE", "LIMIT", offset, count);
    }

    @Override
    public RFuture<V> takeFirstAsync() {
        return pollFirstAsync(0, TimeUnit.SECONDS);
    }

    @Override
    public RFuture<V> takeLastAsync() {
        return pollLastAsync(0, TimeUnit.SECONDS);
    }

    @Override
    public V takeFirst() {
        return get(takeFirstAsync());
    }

    @Override
    public V takeLast() {
        return get(takeLastAsync());
    }

    @Override
    public int subscribeOnFirstElements(Consumer<V> consumer) {
        return commandExecutor.getConnectionManager().getElementsSubscribeService().subscribeOnElements(this::takeFirstAsync, consumer);
    }

    @Override
    public int subscribeOnLastElements(Consumer<V> consumer) {
        return commandExecutor.getConnectionManager().getElementsSubscribeService().subscribeOnElements(this::takeLastAsync, consumer);
    }

    @Override
    public void unsubscribe(int listenerId) {
        commandExecutor.getConnectionManager().getElementsSubscribeService().unsubscribe(listenerId);
    }

    @Override
    public Stream<V> stream() {
        return toStream(iterator());
    }

    @Override
    public Stream<V> stream(String pattern) {
        return toStream(iterator(pattern));
    }

    @Override
    public Stream<V> stream(int count) {
        return toStream(iterator(count));
    }

    @Override
    public Stream<V> stream(String pattern, int count) {
        return toStream(iterator(pattern, count));
    }

    @Override
    public int addListener(ObjectListener listener, Integer previous) {
        if (listener instanceof ScoredSortedSetAddListener) {
            previous = addListener("__keyevent@*:zadd", (ScoredSortedSetAddListener) listener, ScoredSortedSetAddListener::onAdd, previous);
        }
        return super.addListener(listener, previous);
    }

    @Override
    public RFuture<Integer> addListenerAsync(ObjectListener listener) {
        if (listener instanceof ScoredSortedSetAddListener) {
            return addListenerAsync("__keyevent@*:zadd", (ScoredSortedSetAddListener) listener, ScoredSortedSetAddListener::onAdd);
        }
        return super.addListenerAsync(listener);
    }

    @Override
    public void removeListener(int listenerId) {
        RPatternTopic expiredTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:zadd");
        expiredTopic.removeListener(listenerId);

        super.removeListener(listenerId);
    }

    @Override
    public RFuture<Void> removeListenerAsync(int listenerId) {
        RPromise<Void> result = new RedissonPromise<>();
        CountableListener<Void> listener = new CountableListener<>(result, null, 3);

        RPatternTopic setTopic = new RedissonPatternTopic(StringCodec.INSTANCE, commandExecutor, "__keyevent@*:zadd");
        setTopic.removeListenerAsync(listenerId).onComplete(listener);
        removeListenersAsync(listenerId, listener);
        return result;
    }

}
