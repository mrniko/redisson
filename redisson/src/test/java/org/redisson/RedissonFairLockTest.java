package org.redisson;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.redisson.rule.TestUtil.testMultiInstanceConcurrency;
import static org.redisson.rule.TestUtil.testSingleInstanceConcurrency;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

import org.junit.Assert;
import org.junit.Test;
import org.redisson.api.RLock;

public class RedissonFairLockTest extends AbstractBaseTest {

    @Test
    public void testTryLockWait() throws InterruptedException {
        testSingleInstanceConcurrency(redissonRule, 1, r -> {
            RLock lock = r.getFairLock("lock");
            lock.lock();
        });

        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        
        long startTime = System.currentTimeMillis();
        lock.tryLock(3, TimeUnit.SECONDS);
        assertThat(System.currentTimeMillis() - startTime).isBetween(2990L, 3100L);
    }
    
    @Test
    public void testDelete() {
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        Assert.assertFalse(lock.delete());

        lock.lock();
        Assert.assertTrue(lock.delete());
    }

    @Test
    public void testForceUnlock() {
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        lock.lock();
        lock.forceUnlock();
        Assert.assertFalse(lock.isLocked());

        lock = redissonRule.getSharedClient().getFairLock("lock");
        Assert.assertFalse(lock.isLocked());
    }

    @Test
    public void testExpire() throws InterruptedException {
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        lock.lock(2, TimeUnit.SECONDS);

        final long startTime = System.currentTimeMillis();
        Thread t = new Thread() {
            public void run() {
                RLock lock1 = redissonRule.getSharedClient().getFairLock("lock");
                System.out.println("0");
                lock1.lock();
                System.out.println("1");
                long spendTime = System.currentTimeMillis() - startTime;
                System.out.println(spendTime);
                Assert.assertTrue(spendTime < 2020);
                lock1.unlock();
                System.out.println("3");
            };
        };

        t.start();
        t.join();

        lock.unlock();
    }

    @Test
    public void testAutoExpire() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        testSingleInstanceConcurrency(redissonRule, 1, r -> {
            RLock lock = r.getFairLock("lock");
            lock.lock();
            latch.countDown();
        });

        Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        Thread.sleep(TimeUnit.SECONDS.toMillis(RedissonLock.LOCK_EXPIRATION_INTERVAL_SECONDS + 1));
        Assert.assertFalse("Transient lock has not expired automatically", lock.isLocked());
    }

    @Test
    public void testGetHoldCount() {
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        Assert.assertEquals(0, lock.getHoldCount());
        lock.lock();
        Assert.assertEquals(1, lock.getHoldCount());
        lock.unlock();
        Assert.assertEquals(0, lock.getHoldCount());

        lock.lock();
        lock.lock();
        Assert.assertEquals(2, lock.getHoldCount());
        lock.unlock();
        Assert.assertEquals(1, lock.getHoldCount());
        lock.unlock();
        Assert.assertEquals(0, lock.getHoldCount());
    }

    @Test
    public void testIsHeldByCurrentThreadOtherThread() throws InterruptedException {
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        lock.lock();

        Thread t = new Thread() {
            public void run() {
                RLock lock = redissonRule.getSharedClient().getFairLock("lock");
                Assert.assertFalse(lock.isHeldByCurrentThread());
            };
        };

        t.start();
        t.join();
        lock.unlock();

        Thread t2 = new Thread() {
            public void run() {
                RLock lock = redissonRule.getSharedClient().getFairLock("lock");
                Assert.assertFalse(lock.isHeldByCurrentThread());
            };
        };

        t2.start();
        t2.join();
    }

    @Test
    public void testIsHeldByCurrentThread() {
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        Assert.assertFalse(lock.isHeldByCurrentThread());
        lock.lock();
        Assert.assertTrue(lock.isHeldByCurrentThread());
        lock.unlock();
        Assert.assertFalse(lock.isHeldByCurrentThread());
    }

    @Test
    public void testIsLockedOtherThread() throws InterruptedException {
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        lock.lock();

        Thread t = new Thread() {
            public void run() {
                RLock lock = redissonRule.getSharedClient().getFairLock("lock");
                Assert.assertTrue(lock.isLocked());
            };
        };

        t.start();
        t.join();
        lock.unlock();

        Thread t2 = new Thread() {
            public void run() {
                RLock lock = redissonRule.getSharedClient().getFairLock("lock");
                Assert.assertFalse(lock.isLocked());
            };
        };

        t2.start();
        t2.join();
    }

    @Test
    public void testIsLocked() {
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        Assert.assertFalse(lock.isLocked());
        lock.lock();
        Assert.assertTrue(lock.isLocked());
        lock.unlock();
        Assert.assertFalse(lock.isLocked());
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlockFail() throws InterruptedException {
        RLock lock = redissonRule.getSharedClient().getFairLock("lock");
        Thread t = new Thread() {
            public void run() {
                RLock lock = redissonRule.getSharedClient().getFairLock("lock");
                lock.lock();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }

                lock.unlock();
            };
        };

        t.start();
        t.join(400);

        try {
            lock.unlock();
        } catch (IllegalMonitorStateException e) {
            t.join();
            throw e;
        }
    }


    @Test
    public void testLockUnlock() {
        Lock lock = redissonRule.getSharedClient().getFairLock("lock1");
        lock.lock();
        lock.unlock();

        lock.lock();
        lock.unlock();
    }

    @Test
    public void testReentrancy() throws InterruptedException {
        Lock lock = redissonRule.getSharedClient().getFairLock("lock1");
        Assert.assertTrue(lock.tryLock());
        Assert.assertTrue(lock.tryLock());
        lock.unlock();
        // next row  for test renew expiration tisk.
        //Thread.currentThread().sleep(TimeUnit.SECONDS.toMillis(RedissonLock.LOCK_EXPIRATION_INTERVAL_SECONDS*2));
        Thread thread1 = new Thread() {
            @Override
            public void run() {
                RLock lock1 = redissonRule.getSharedClient().getFairLock("lock1");
                Assert.assertFalse(lock1.tryLock());
            }
        };
        thread1.start();
        thread1.join();
        lock.unlock();
    }


    @Test
    public void testConcurrency_SingleInstance() throws InterruptedException {
        final AtomicInteger lockedCounter = new AtomicInteger();

        int iterations = 15;
        testSingleInstanceConcurrency(redissonRule, iterations, r -> {
            Lock lock = r.getFairLock("testConcurrency_SingleInstance");
            lock.lock();
            lockedCounter.incrementAndGet();
            lock.unlock();
        });

        Assert.assertEquals(iterations, lockedCounter.get());
    }

    @Test
    public void testConcurrencyLoop_MultiInstance() throws InterruptedException {
        final int iterations = 100;
        final AtomicInteger lockedCounter = new AtomicInteger();

        testMultiInstanceConcurrency(redissonRule, 16, r -> {
            for (int i = 0; i < iterations; i++) {
                r.getFairLock("testConcurrency_MultiInstance1").lock();
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                lockedCounter.incrementAndGet();
                r.getFairLock("testConcurrency_MultiInstance1").unlock();
            }
        });

        Assert.assertEquals(16 * iterations, lockedCounter.get());
    }

    @Test
    public void testConcurrency_MultiInstance() throws InterruptedException {
        int iterations = 100;
        final AtomicInteger lockedCounter = new AtomicInteger();

        testMultiInstanceConcurrency(redissonRule, iterations, r -> {
            Lock lock = r.getFairLock("testConcurrency_MultiInstance2");
            lock.lock();
            lockedCounter.incrementAndGet();
            lock.unlock();
        });

        Assert.assertEquals(iterations, lockedCounter.get());
    }
    
    @Test
    public void testConcurrency_MultiInstance_Ordering() throws InterruptedException {
        final ConcurrentLinkedQueue<Thread> queue = new ConcurrentLinkedQueue<>();
        final AtomicInteger lockedCounter = new AtomicInteger();

        int totalThreads = Runtime.getRuntime().availableProcessors()*2;
        for (int i = 0; i < totalThreads; i++) {
            Thread t1 = new Thread(() -> {
                Lock lock = redissonRule.getSharedClient().getFairLock("testConcurrency_MultiInstance2");
                queue.add(Thread.currentThread());
                lock.lock();
                Thread t = queue.poll();
                assertThat(t).isEqualTo(Thread.currentThread());
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                lockedCounter.incrementAndGet();
                lock.unlock();
            });
            Thread.sleep(10);
            t1.start();
        }
        
        await().atMost(30, TimeUnit.SECONDS).until(() -> assertThat(lockedCounter.get()).isEqualTo(totalThreads));
    }


}
