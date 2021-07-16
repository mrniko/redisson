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

import io.netty.buffer.ByteBufUtil;
import org.redisson.api.RFuture;
import org.redisson.api.RSemaphore;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.RPromise;
import org.redisson.misc.RedissonPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public abstract class RedissonBaseAdder<T extends Number> extends RedissonExpirable {

    private static class ResetListener implements BiConsumer<Long, Throwable> {
        
        private final RPromise<Void> result;
        private final RSemaphore semaphore;

        ResetListener(RSemaphore semaphore, RPromise<Void> result) {
            this.result = result;
            this.semaphore = semaphore;
        }

        @Override
        public void accept(Long t, Throwable u) {
            if (u != null) {
                result.tryFailure(u);
                return;
            }
            
            acquireAsync(t.intValue()).onComplete((res, e) -> {
                if (e != null) {
                    result.tryFailure(e);
                    return;
                }

                semaphore.deleteAsync().onComplete((re, exc) -> {
                    if (exc != null) {
                        result.tryFailure(exc);
                        return;
                    }

                    result.trySuccess(res);
                });
            });
        }

        protected RFuture<Void> acquireAsync(int value) {
            return semaphore.acquireAsync(value);
        }
    }

    private class SumListener implements BiConsumer<Long, Throwable> {
        
        private final RPromise<T> result;
        private final RSemaphore semaphore;
        private final String id;

        SumListener(String id, RSemaphore semaphore, RPromise<T> result) {
            this.result = result;
            this.semaphore = semaphore;
            this.id = id;
        }

        @Override
        public void accept(Long t, Throwable u) {
            if (u != null) {
                result.tryFailure(u);
                return;
            }

            acquireAsync(t.intValue()).onComplete((r, e) -> {
                if (e != null) {
                    result.tryFailure(e);
                    return;
                }

                RFuture<T> valueFuture = getAndDeleteAsync(id);
                valueFuture.onComplete((res, ex) -> {
                    if (ex != null) {
                        result.tryFailure(ex);
                        return;
                    }

                    semaphore.deleteAsync().onComplete((re, exc) -> {
                        if (exc != null) {
                            result.tryFailure(exc);
                            return;
                        }

                        result.trySuccess(res);
                    });
                });
            });
        }

        protected RFuture<Void> acquireAsync(int value) {
            return semaphore.acquireAsync(value);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RedissonBaseAdder.class);
    
    private static final String CLEAR_MSG = "0";
    private static final String SUM_MSG = "1";

    private final RedissonClient redisson;
    private final RTopic topic;
    private final int listenerId;
    
    public RedissonBaseAdder(CommandAsyncExecutor connectionManager, String name, RedissonClient redisson) {
        super(connectionManager, name);
        
        topic = redisson.getTopic(suffixName(getRawName(), "topic"), StringCodec.INSTANCE);
        this.redisson = redisson;
        listenerId = topic.addListener(String.class, (channel, msg) -> {
            String[] parts = msg.split(":");
            String id = parts[1];

            RSemaphore semaphore = getSemaphore(id);
            if (parts[0].equals(SUM_MSG)) {
                RFuture<T> addAndGetFuture = addAndGetAsync(id);
                addAndGetFuture.onComplete((res, e) -> {
                    if (e != null) {
                        log.error("Can't increase sum", e);
                        return;
                    }

                    semaphore.releaseAsync().onComplete((r, ex) -> {
                        if (ex != null) {
                            log.error("Can't release semaphore", ex);
                        }
                    });
                });
            }

            if (parts[0].equals(CLEAR_MSG)) {
                doReset();
                semaphore.releaseAsync().onComplete((res, e) -> {
                    if (e != null) {
                        log.error("Can't release semaphore", e);
                    }
                });
            }
        });
    }

    protected abstract void doReset();

    private String generateId() {
        byte[] id = new byte[16];
        ThreadLocalRandom.current().nextBytes(id);
        return ByteBufUtil.hexDump(id);
    }

    public void reset() {
        get(resetAsync());
    }
    
    public void reset(long timeout, TimeUnit timeUnit) {
        get(resetAsync(timeout, timeUnit));
    }
    
    public RFuture<T> sumAsync() {
        RPromise<T> result = new RedissonPromise<T>();

        String id = generateId();
        RFuture<Long> future = topic.publishAsync(SUM_MSG + ":" + id);
        RSemaphore semaphore = getSemaphore(id);
        future.onComplete(new SumListener(id, semaphore, result));
        
        return result;
    }

    private RSemaphore getSemaphore(String id) {
        return redisson.getSemaphore(suffixName(getRawName(), id + ":semaphore"));
    }

    protected String getCounterName(String id) {
        return suffixName(getRawName(), id + ":counter");
    }

    public RFuture<T> sumAsync(long timeout, TimeUnit timeUnit) {
        RPromise<T> result = new RedissonPromise<T>();

        String id = generateId();
        RFuture<Long> future = topic.publishAsync(SUM_MSG + ":" + id);
        RSemaphore semaphore = getSemaphore(id);
        future.onComplete(new SumListener(id, semaphore, result) {
            @Override
            protected RFuture<Void> acquireAsync(int value) {
                return tryAcquire(semaphore, timeout, timeUnit, value);
            }

        });
        
        return result;
    }

    protected RFuture<Void> tryAcquire(RSemaphore semaphore, long timeout, TimeUnit timeUnit, int value) {
        RPromise<Void> acquireResult = new RedissonPromise<>();
        semaphore.tryAcquireAsync(value, timeout, timeUnit).onComplete((res, e) -> {
            if (e != null) {
                acquireResult.tryFailure(e);
                return;
            }
            
            if (res) {
                acquireResult.trySuccess(null);
            } else {
                acquireResult.tryFailure(new TimeoutException());
            }
        });
        return acquireResult;
    }

    public RFuture<Void> resetAsync() {
        RPromise<Void> result = new RedissonPromise<>();

        String id = generateId();
        RFuture<Long> future = topic.publishAsync(CLEAR_MSG + ":" + id);
        RSemaphore semaphore = getSemaphore(id);
        future.onComplete(new ResetListener(semaphore, result));
        
        return result;
    }
    
    public RFuture<Void> resetAsync(long timeout, TimeUnit timeUnit) {
        RPromise<Void> result = new RedissonPromise<>();

        String id = generateId();
        RFuture<Long> future = topic.publishAsync(CLEAR_MSG + ":" + id);
        RSemaphore semaphore = getSemaphore(id);
        future.onComplete(new ResetListener(semaphore, result) {
            @Override
            protected RFuture<Void> acquireAsync(int value) {
                return tryAcquire(semaphore, timeout, timeUnit, value);
            }
        });
        
        return result;
    }

    @Override
    public boolean delete() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected RFuture<Boolean> deleteAsync(String... keys) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        throw new UnsupportedOperationException();
    }

    public void destroy() {
        topic.removeListener(listenerId);
    }

    protected abstract RFuture<T> addAndGetAsync(String id);

    protected abstract RFuture<T> getAndDeleteAsync(String id);

}
