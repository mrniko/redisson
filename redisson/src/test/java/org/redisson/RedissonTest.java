package org.redisson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.redisson.BaseTest.createInstance;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.redisson.ClusterRunner.ClusterProcesses;
import org.redisson.RedisRunner.RedisProcess;
import org.redisson.api.ClusterNode;
import org.redisson.api.Node;
import org.redisson.api.Node.InfoSection;
import org.redisson.api.NodeType;
import org.redisson.api.NodesGroup;
import org.redisson.api.RBucket;
import org.redisson.api.RBuckets;
import org.redisson.api.RFuture;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RMapCache;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisClientConfig;
import org.redisson.client.RedisClientConfig.TlsVersion;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.RedisOutOfMemoryException;
import org.redisson.client.codec.BaseCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.handler.State;
import org.redisson.client.protocol.Decoder;
import org.redisson.client.protocol.Encoder;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.Time;
import org.redisson.cluster.ClusterNodeInfo;
import org.redisson.cluster.ClusterNodeInfo.Flag;
import org.redisson.codec.FstCodec;
import org.redisson.codec.JsonJacksonCodec;
import org.redisson.codec.SerializationCodec;
import org.redisson.config.Config;
import org.redisson.config.ReadMode;
import org.redisson.config.SubscriptionMode;
import org.redisson.connection.CRC16;
import org.redisson.connection.ConnectionListener;
import org.redisson.connection.MasterSlaveConnectionManager;
import org.redisson.connection.balancer.RandomLoadBalancer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.CharsetUtil;
import net.bytebuddy.utility.RandomString;

public class RedissonTest {

    protected RedissonClient redisson;
    protected static RedissonClient defaultRedisson;
    
//    @Test
    public void testLeak() throws InterruptedException {
        Config config = new Config();
        config.useSingleServer()
              .setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());

        RedissonClient localRedisson = Redisson.create(config);

        String key = RandomString.make(120);
        for (int i = 0; i < 500; i++) {
            RMapCache<String, String> cache = localRedisson.getMapCache("mycache");
            RLock keyLock = cache.getLock(key);
            keyLock.lockInterruptibly(10, TimeUnit.SECONDS);
            try {
                cache.get(key);
                cache.put(key, RandomString.make(4*1024*1024), 5, TimeUnit.SECONDS);
            } finally {
                if (keyLock != null) {
                    keyLock.unlock();
                }
            }
        }
        

    }
    
    @Test
    public void testDecoderError() {
        redisson.getBucket("testbucket", new StringCodec()).set("{INVALID JSON!}");

        for (int i = 0; i < 256; i++) {
          try {
              redisson.getBucket("testbucket", new JsonJacksonCodec()).get();
              Assert.fail();
          } catch (Exception e) {
              // skip
          }
        }

        redisson.getBucket("testbucket2").set("should work");
    }
    
    @Test
    public void testSmallPool() throws InterruptedException {
        Config config = new Config();
        config.useSingleServer()
              .setConnectionMinimumIdleSize(3)
              .setConnectionPoolSize(3)
              .setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());

        RedissonClient localRedisson = Redisson.create(config);
        
        RMap<String, String> map = localRedisson.getMap("test");
        
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()*2);
        long start = System.currentTimeMillis();
        int iterations = 500_000;
        for (int i = 0; i < iterations; i++) {
            final int j = i;
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    map.put("" + j, "" + j);
                }
            });
        }
        
        executor.shutdown();
        Assert.assertTrue(executor.awaitTermination(10, TimeUnit.MINUTES));
        
        assertThat(map.size()).isEqualTo(iterations);
        
        localRedisson.shutdown();
    }
    
    @BeforeClass
    public static void beforeClass() throws IOException, InterruptedException {
        if (!RedissonRuntimeEnvironment.isTravis) {
            RedisRunner.startDefaultRedisServerInstance();
            defaultRedisson = BaseTest.createInstance();
        }
    }

    @AfterClass
    public static void afterClass() throws IOException, InterruptedException {
        if (!RedissonRuntimeEnvironment.isTravis) {
            RedisRunner.shutDownDefaultRedisServerInstance();
            defaultRedisson.shutdown();
        }
    }

    @Before
    public void before() throws IOException, InterruptedException {
        if (RedissonRuntimeEnvironment.isTravis) {
            RedisRunner.startDefaultRedisServerInstance();
            redisson = createInstance();
        } else {
            if (redisson == null) {
                redisson = defaultRedisson;
            }
            redisson.getKeys().flushall();
        }
    }

    @After
    public void after() throws InterruptedException {
        if (RedissonRuntimeEnvironment.isTravis) {
            redisson.shutdown();
            RedisRunner.shutDownDefaultRedisServerInstance();
        }
    }
    
    public static class Dummy {
        private String field;
    }
    
    @Test
    public void testNextResponseAfterDecoderError() throws Exception {
        Config config = new Config();
        config.useSingleServer()
                .setConnectionMinimumIdleSize(1)
                .setConnectionPoolSize(1)
              .setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());

        RedissonClient redisson = Redisson.create(config);
        
        setJSONValue(redisson, "test1", "test1");
        setStringValue(redisson, "test2", "test2");
        setJSONValue(redisson, "test3", "test3");
        try {
            RBuckets buckets = redisson.getBuckets(new JsonJacksonCodec());
            buckets.get("test2", "test1");
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertThat(getStringValue(redisson, "test3")).isEqualTo("\"test3\"");
        
        redisson.shutdown();
    }

    public void setJSONValue(RedissonClient redisson, String key, Object t) {
        RBucket<Object> test1 = redisson.getBucket(key, new JsonJacksonCodec());
        test1.set(t);
    }

    public void setStringValue(RedissonClient redisson, String key, Object t) {
        RBucket<Object> test1 = redisson.getBucket(key, new StringCodec());
        test1.set(t);
    }


    public Object getStringValue(RedissonClient redisson, String key) {
        RBucket<Object> test1 = redisson.getBucket(key, new StringCodec());
        return test1.get();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSer() {
        Config config = new Config();
        config.useSingleServer().setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());
        config.setCodec(new SerializationCodec());
        RedissonClient r = Redisson.create(config);
        r.getMap("test").put("1", new Dummy());
    }
    
    @Test(expected = RedisOutOfMemoryException.class)
    public void testMemoryScript() throws IOException, InterruptedException {
        RedisProcess p = redisTestSmallMemory();

        Config config = new Config();
        config.useSingleServer().setAddress(p.getRedisServerAddressAndPort()).setTimeout(100000);

        RedissonClient r = null;
        try {
            r = Redisson.create(config);
            r.getKeys().flushall();
            for (int i = 0; i < 10000; i++) {
                r.getMap("test").put("" + i, "" + i);
            }
        } finally {
            r.shutdown();
            p.stop();
        }
    }

    @Test(expected = RedisOutOfMemoryException.class)
    public void testMemoryCommand() throws IOException, InterruptedException {
        RedisProcess p = redisTestSmallMemory();

        Config config = new Config();
        config.useSingleServer().setAddress(p.getRedisServerAddressAndPort()).setTimeout(100000);

        RedissonClient r = null;
        try {
            r = Redisson.create(config);
            r.getKeys().flushall();
            for (int i = 0; i < 10000; i++) {
                r.getMap("test").fastPut("" + i, "" + i);
            }
        } finally {
            r.shutdown();
            p.stop();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigValidation() {
        Config redissonConfig = new Config();
        redissonConfig.useSingleServer()
        .setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort())
        .setConnectionPoolSize(2);
        Redisson.create(redissonConfig);        
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTlsConfig() {
        TlsVersion tlsVersion = RedisClientConfig.TlsVersion.TLSv2;
        Config radissonConfig = new Config();
        radissonConfig.useSingleServer()
                .setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort())
                .setTlsVersion(tlsVersion);
        Assert.assertEquals(tlsVersion, radissonConfig.useSingleServer().getTlsVersion());
        RedissonClient r = Redisson.create(radissonConfig);
        Assert.assertEquals(tlsVersion, r.getConfig().useSingleServer().getTlsVersion());
    }

    @Test
    public void testConnectionListener() throws IOException, InterruptedException, TimeoutException {

        final RedisProcess p = redisTestConnection();

        final AtomicInteger connectCounter = new AtomicInteger();
        final AtomicInteger disconnectCounter = new AtomicInteger();

        Config config = new Config();
        config.useSingleServer().setAddress(p.getRedisServerAddressAndPort());

        RedissonClient r = Redisson.create(config);

        int id = r.getNodesGroup().addConnectionListener(new ConnectionListener() {

            @Override
            public void onDisconnect(InetSocketAddress addr) {
                assertThat(addr).isEqualTo(new InetSocketAddress(p.getRedisServerBindAddress(), p.getRedisServerPort()));
                disconnectCounter.incrementAndGet();
            }

            @Override
            public void onConnect(InetSocketAddress addr) {
                assertThat(addr).isEqualTo(new InetSocketAddress(p.getRedisServerBindAddress(), p.getRedisServerPort()));
                connectCounter.incrementAndGet();
            }
        });

        assertThat(id).isNotZero();
        
        r.getBucket("1").get();
        Assert.assertEquals(0, p.stop());
        
        await().atMost(2, TimeUnit.SECONDS).until(() -> disconnectCounter.get() == 1);
        
        try {
            r.getBucket("1").get();
        } catch (Exception e) {
        }
        
        assertThat(connectCounter.get()).isEqualTo(0);
        assertThat(disconnectCounter.get()).isEqualTo(1);

        RedisProcess pp = new RedisRunner()
                .nosave()
                .port(p.getRedisServerPort())
                .randomDir()
                .run();

        r.getBucket("1").get();

        assertThat(connectCounter.get()).isEqualTo(1);
        assertThat(disconnectCounter.get()).isEqualTo(1);

        r.shutdown();
        Assert.assertEquals(0, pp.stop());
    }
    
    @Test
    public void testFailoverInSentinel() throws Exception {
        RedisRunner.RedisProcess master = new RedisRunner()
                .nosave()
                .randomDir()
                .run();
        RedisRunner.RedisProcess slave1 = new RedisRunner()
                .port(6380)
                .nosave()
                .randomDir()
                .slaveof("127.0.0.1", 6379)
                .run();
        RedisRunner.RedisProcess slave2 = new RedisRunner()
                .port(6381)
                .nosave()
                .randomDir()
                .slaveof("127.0.0.1", 6379)
                .run();
        RedisRunner.RedisProcess sentinel1 = new RedisRunner()
                .nosave()
                .randomDir()
                .port(26379)
                .sentinel()
                .sentinelMonitor("myMaster", "127.0.0.1", 6379, 2)
                .run();
        RedisRunner.RedisProcess sentinel2 = new RedisRunner()
                .nosave()
                .randomDir()
                .port(26380)
                .sentinel()
                .sentinelMonitor("myMaster", "127.0.0.1", 6379, 2)
                .run();
        RedisRunner.RedisProcess sentinel3 = new RedisRunner()
                .nosave()
                .randomDir()
                .port(26381)
                .sentinel()
                .sentinelMonitor("myMaster", "127.0.0.1", 6379, 2)
                .run();
        
        Thread.sleep(5000); 
        
        Config config = new Config();
        config.useSentinelServers()
            .setLoadBalancer(new RandomLoadBalancer())
            .addSentinelAddress(sentinel3.getRedisServerAddressAndPort()).setMasterName("myMaster");
        RedissonClient redisson = Redisson.create(config);
        
        List<RFuture<?>> futures = new ArrayList<RFuture<?>>();
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    RFuture<?> f1 = redisson.getBucket("i" + i).getAsync();
                    RFuture<?> f2 = redisson.getBucket("i" + i).setAsync("");
                    RFuture<?> f3 = redisson.getTopic("topic").publishAsync("testmsg");
                    futures.add(f1);
                    futures.add(f2);
                    futures.add(f3);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                latch.countDown();
            };
        };
        t.start();
        
        master.stop();
        System.out.println("master " + master.getRedisServerAddressAndPort() + " stopped!");
        
        Thread.sleep(TimeUnit.SECONDS.toMillis(70));
        
        master = new RedisRunner()
                .port(master.getRedisServerPort())
                .nosave()
                .randomDir()
                .run();

        System.out.println("master " + master.getRedisServerAddressAndPort() + " started!");
        
        
        Thread.sleep(15000);
        
        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();

        int errors = 0;
        int success = 0;
        int readonlyErrors = 0;
        
        for (RFuture<?> rFuture : futures) {
            rFuture.awaitUninterruptibly();
            if (!rFuture.isSuccess()) {
                System.out.println("cause " + rFuture.cause());
                if (rFuture.cause().getMessage().contains("READONLY You can't write against")) {
                    readonlyErrors++;
                }
                errors++;
            } else {
                success++;
            }
        }
        
        System.out.println("errors " + errors + " success " + success + " readonly " + readonlyErrors);
        
        assertThat(errors).isLessThan(600);
        assertThat(readonlyErrors).isZero();
        
        redisson.shutdown();
        sentinel1.stop();
        sentinel2.stop();
        sentinel3.stop();
        master.stop();
        slave1.stop();
        slave2.stop();
    }
    
    public static class SlowCodec extends BaseCodec {

        private final Encoder encoder = new Encoder() {
            @Override
            public ByteBuf encode(Object in) throws IOException {
                ByteBuf out = ByteBufAllocator.DEFAULT.buffer();
                out.writeCharSequence(in.toString(), CharsetUtil.UTF_8);
                return out;
            }
        };

        public final Decoder<Object> decoder = new Decoder<Object>() {
            @Override
            public Object decode(ByteBuf buf, State state) throws IOException {
                String str = buf.toString(CharsetUtil.UTF_8);
                buf.readerIndex(buf.readableBytes());
                try {
                    Thread.sleep(2500);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                return str;
            }
        };

        public SlowCodec() {
        }
        
        public SlowCodec(ClassLoader classLoader) {
            this();
        }


        @Override
        public Decoder<Object> getValueDecoder() {
            return decoder;
        }

        @Override
        public Encoder getValueEncoder() {
            return encoder;
        }

    }
    
    @Test
    public void testDecoderInExecutor() throws Exception {
        Config config = new Config();
        config.setCodec(new SlowCodec());
        config.setReferenceEnabled(false);
        config.setThreads(32);
        config.setNettyThreads(8);
        config.setDecodeInExecutor(true);
        config.useSingleServer()
            .setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());
        RedissonClient redisson = Redisson.create(config);
        
        CountDownLatch latch = new CountDownLatch(16);
        AtomicBoolean hasErrors = new AtomicBoolean();
        for (int i = 0; i < 16; i++) {
            Thread t = new Thread() {
                public void run() {
                    for (int i = 0; i < 10; i++) {
                        try {
                            redisson.getBucket("123").set("1");
                            redisson.getBucket("123").get();
                            if (hasErrors.get()) {
                                latch.countDown();
                                return;
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            hasErrors.set(true);
                        }
                        
                    }
                    latch.countDown();
                };
            };
            t.start();
        }
        
        assertThat(latch.await(60, TimeUnit.SECONDS)).isTrue();
        assertThat(hasErrors).isFalse();
        
        redisson.shutdown();
    }

    
    @Test
    public void testFailoverWithoutErrorsInCluster() throws Exception {
        RedisRunner master1 = new RedisRunner().port(6890).randomDir().nosave();
        RedisRunner master2 = new RedisRunner().port(6891).randomDir().nosave();
        RedisRunner master3 = new RedisRunner().port(6892).randomDir().nosave();
        RedisRunner slave1 = new RedisRunner().port(6900).randomDir().nosave();
        RedisRunner slave2 = new RedisRunner().port(6901).randomDir().nosave();
        RedisRunner slave3 = new RedisRunner().port(6902).randomDir().nosave();
        
        ClusterRunner clusterRunner = new ClusterRunner()
                .addNode(master1, slave1)
                .addNode(master2, slave2)
                .addNode(master3, slave3);
        ClusterProcesses process = clusterRunner.run();
        
        Config config = new Config();
        config.useClusterServers()
        .setRetryAttempts(30)
        .setReadMode(ReadMode.MASTER)
        .setSubscriptionMode(SubscriptionMode.MASTER)
        .setLoadBalancer(new RandomLoadBalancer())
        .addNodeAddress(process.getNodes().stream().findAny().get().getRedisServerAddressAndPort());
        RedissonClient redisson = Redisson.create(config);
       
        RedisProcess master = process.getNodes().stream().filter(x -> x.getRedisServerPort() == master1.getPort()).findFirst().get();
        
        List<RFuture<?>> futures = new ArrayList<RFuture<?>>();

        Set<InetSocketAddress> oldMasters = new HashSet<>();
        Collection<ClusterNode> masterNodes = redisson.getClusterNodesGroup().getNodes(NodeType.MASTER);
        for (ClusterNode clusterNode : masterNodes) {
            oldMasters.add(clusterNode.getAddr());
        }
        
        master.stop();

        for (int j = 0; j < 2000; j++) {
            RFuture<?> f2 = redisson.getBucket("" + j).setAsync("");
            futures.add(f2);
        }
        
        System.out.println("master " + master.getRedisServerAddressAndPort() + " has been stopped!");
        
        Thread.sleep(TimeUnit.SECONDS.toMillis(30));
        
        RedisProcess newMaster = null;
        Collection<ClusterNode> newMasterNodes = redisson.getClusterNodesGroup().getNodes(NodeType.MASTER);
        for (ClusterNode clusterNode : newMasterNodes) {
            if (!oldMasters.contains(clusterNode.getAddr())) {
                newMaster = process.getNodes().stream().filter(x -> x.getRedisServerPort() == clusterNode.getAddr().getPort()).findFirst().get();
                break;
            }
        }
        
        assertThat(newMaster).isNotNull();
        
        for (RFuture<?> rFuture : futures) {
            rFuture.awaitUninterruptibly();
            if (!rFuture.isSuccess()) {
                Assert.fail();
            }
        }
        
        redisson.shutdown();
        process.shutdown();
    }

    @Test
    public void testFailoverInCluster() throws Exception {
        RedisRunner master1 = new RedisRunner().port(6890).randomDir().nosave();
        RedisRunner master2 = new RedisRunner().port(6891).randomDir().nosave();
        RedisRunner master3 = new RedisRunner().port(6892).randomDir().nosave();
        RedisRunner slave1 = new RedisRunner().port(6900).randomDir().nosave();
        RedisRunner slave2 = new RedisRunner().port(6901).randomDir().nosave();
        RedisRunner slave3 = new RedisRunner().port(6902).randomDir().nosave();
        RedisRunner slave4 = new RedisRunner().port(6903).randomDir().nosave();
        
        ClusterRunner clusterRunner = new ClusterRunner()
                .addNode(master1, slave1, slave4)
                .addNode(master2, slave2)
                .addNode(master3, slave3);
        ClusterProcesses process = clusterRunner.run();
        
        Thread.sleep(5000); 
        
        Config config = new Config();
        config.useClusterServers()
        .setLoadBalancer(new RandomLoadBalancer())
        .addNodeAddress(process.getNodes().stream().findAny().get().getRedisServerAddressAndPort());
        RedissonClient redisson = Redisson.create(config);
       
        RedisProcess master = process.getNodes().stream().filter(x -> x.getRedisServerPort() == master1.getPort()).findFirst().get();
        
        List<RFuture<?>> futures = new ArrayList<RFuture<?>>();
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread() {
            public void run() {
                for (int i = 0; i < 2000; i++) {
                    RFuture<?> f1 = redisson.getBucket("i" + i).getAsync();
                    RFuture<?> f2 = redisson.getBucket("i" + i).setAsync("");
                    RFuture<?> f3 = redisson.getTopic("topic").publishAsync("testmsg");
                    futures.add(f1);
                    futures.add(f2);
                    futures.add(f3);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    if (i % 100 == 0) {
                        System.out.println("step: " + i);
                    }
                }
                latch.countDown();
            };
        };
        t.start();
        t.join(1000);

        Set<InetSocketAddress> oldMasters = new HashSet<>();
        Collection<ClusterNode> masterNodes = redisson.getClusterNodesGroup().getNodes(NodeType.MASTER);
        for (ClusterNode clusterNode : masterNodes) {
            oldMasters.add(clusterNode.getAddr());
        }
        
        master.stop();
        System.out.println("master " + master.getRedisServerAddressAndPort() + " has been stopped!");
        
        Thread.sleep(TimeUnit.SECONDS.toMillis(90));
        
        RedisProcess newMaster = null;
        Collection<ClusterNode> newMasterNodes = redisson.getClusterNodesGroup().getNodes(NodeType.MASTER);
        for (ClusterNode clusterNode : newMasterNodes) {
            if (!oldMasters.contains(clusterNode.getAddr())) {
                newMaster = process.getNodes().stream().filter(x -> x.getRedisServerPort() == clusterNode.getAddr().getPort()).findFirst().get();
                break;
            }
        }
        
        assertThat(newMaster).isNotNull();
        
        Thread.sleep(30000);
        
        newMaster.stop();

        System.out.println("new master " + newMaster.getRedisServerAddressAndPort() + " has been stopped!");
        
        Thread.sleep(TimeUnit.SECONDS.toMillis(80));

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();

        int errors = 0;
        int success = 0;
        int readonlyErrors = 0;
        
        for (RFuture<?> rFuture : futures) {
            rFuture.awaitUninterruptibly();
            if (!rFuture.isSuccess()) {
                errors++;
            } else {
                success++;
            }
        }
        
        System.out.println("errors " + errors + " success " + success);

        for (RFuture<?> rFuture : futures) {
            if (rFuture.isSuccess()) {
                System.out.println(rFuture.isSuccess());
            } else {
                rFuture.cause().printStackTrace();
            }
        }
        
        assertThat(readonlyErrors).isZero();
        
        redisson.shutdown();
        process.shutdown();
    }

    
    @Test
    public void testReconnection() throws IOException, InterruptedException, TimeoutException {
        RedisProcess runner = new RedisRunner()
                .appendonly(true)
                .randomDir()
                .randomPort()
                .run();

        Config config = new Config();
        config.useSingleServer().setAddress(runner.getRedisServerAddressAndPort());

        RedissonClient r = Redisson.create(config);
        
        r.getBucket("myBucket").set(1);
        assertThat(r.getBucket("myBucket").get()).isEqualTo(1);
        
        Assert.assertEquals(0, runner.stop());
        
        AtomicBoolean hasError = new AtomicBoolean();
        try {
            r.getBucket("myBucket").get();
        } catch (Exception e) {
            // skip error
            hasError.set(true);
        }

        assertThat(hasError.get()).isTrue();
        
        RedisProcess pp = new RedisRunner()
                .appendonly(true)
                .port(runner.getRedisServerPort())
                .dir(runner.getDefaultDir())
                .run();

        assertThat(r.getBucket("myBucket").get()).isEqualTo(1);

        r.shutdown();

        Assert.assertEquals(0, pp.stop());
    }


    @Test
    public void testShutdown() {
        Config config = new Config();
        config.useSingleServer().setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());

        RedissonClient r = Redisson.create(config);
        Assert.assertFalse(r.isShuttingDown());
        Assert.assertFalse(r.isShutdown());
        r.shutdown();
        Assert.assertTrue(r.isShuttingDown());
        Assert.assertTrue(r.isShutdown());
    }

    @Test
    public void testNode() {
        Node node = redisson.getNodesGroup().getNode(RedisRunner.getDefaultRedisServerBindAddressAndPort());
        assertThat(node).isNotNull();
        
        assertThat(node.info(InfoSection.ALL)).isNotEmpty();
    }
    
    @Test
    public void testInfo() {
        Node node = redisson.getNodesGroup().getNodes().iterator().next();

        Map<String, String> allResponse = node.info(InfoSection.ALL);
        assertThat(allResponse).containsKeys("redis_version", "connected_clients");
        
        Map<String, String> defaultResponse = node.info(InfoSection.DEFAULT);
        assertThat(defaultResponse).containsKeys("redis_version", "connected_clients");
        
        Map<String, String> serverResponse = node.info(InfoSection.SERVER);
        assertThat(serverResponse).containsKey("redis_version");
        
        Map<String, String> clientsResponse = node.info(InfoSection.CLIENTS);
        assertThat(clientsResponse).containsKey("connected_clients");

        Map<String, String> memoryResponse = node.info(InfoSection.MEMORY);
        assertThat(memoryResponse).containsKey("used_memory_human");
        
        Map<String, String> persistenceResponse = node.info(InfoSection.PERSISTENCE);
        assertThat(persistenceResponse).containsKey("rdb_last_save_time");

        Map<String, String> statsResponse = node.info(InfoSection.STATS);
        assertThat(statsResponse).containsKey("pubsub_patterns");
        
        Map<String, String> replicationResponse = node.info(InfoSection.REPLICATION);
        assertThat(replicationResponse).containsKey("repl_backlog_first_byte_offset");
        
        Map<String, String> cpuResponse = node.info(InfoSection.CPU);
        assertThat(cpuResponse).containsKey("used_cpu_sys");

        Map<String, String> commandStatsResponse = node.info(InfoSection.COMMANDSTATS);
        assertThat(commandStatsResponse).containsKey("cmdstat_flushall");

        Map<String, String> clusterResponse = node.info(InfoSection.CLUSTER);
        assertThat(clusterResponse).containsKey("cluster_enabled");

        Map<String, String> keyspaceResponse = node.info(InfoSection.KEYSPACE);
        assertThat(keyspaceResponse).isEmpty();
    }
    
    @Test
    public void testTime() {
        NodesGroup<Node> nodes = redisson.getNodesGroup();
        Assert.assertEquals(1, nodes.getNodes().size());
        Iterator<Node> iter = nodes.getNodes().iterator();

        Node node1 = iter.next();
        Time time = node1.time();
        assertThat(time.getSeconds()).isGreaterThan(time.getMicroseconds());
        assertThat(time.getSeconds()).isGreaterThan(1000000000);
        assertThat(time.getMicroseconds()).isGreaterThan(10000);
    }

    @Test
    public void testPing() {
        NodesGroup<Node> nodes = redisson.getNodesGroup();
        Assert.assertEquals(1, nodes.getNodes().size());
        Iterator<Node> iter = nodes.getNodes().iterator();

        Node node1 = iter.next();
        Assert.assertTrue(node1.ping());

        Assert.assertTrue(nodes.pingAll());
    }

    @Test
    public void testSentinelStartup() throws Exception {
        RedisRunner.RedisProcess master = new RedisRunner()
                .nosave()
                .randomDir()
                .run();
        RedisRunner.RedisProcess slave1 = new RedisRunner()
                .port(6380)
                .nosave()
                .randomDir()
                .slaveof("127.0.0.1", 6379)
                .run();
        RedisRunner.RedisProcess slave2 = new RedisRunner()
                .port(6381)
                .nosave()
                .randomDir()
                .slaveof("127.0.0.1", 6379)
                .run();
        RedisRunner.RedisProcess sentinel1 = new RedisRunner()
                .nosave()
                .randomDir()
                .port(26379)
                .sentinel()
                .sentinelMonitor("myMaster", "127.0.0.1", 6379, 2)
                .run();
        RedisRunner.RedisProcess sentinel2 = new RedisRunner()
                .nosave()
                .randomDir()
                .port(26380)
                .sentinel()
                .sentinelMonitor("myMaster", "127.0.0.1", 6379, 2)
                .run();
        RedisRunner.RedisProcess sentinel3 = new RedisRunner()
                .nosave()
                .randomDir()
                .port(26381)
                .sentinel()
                .sentinelMonitor("myMaster", "127.0.0.1", 6379, 2)
                .run();
        
        Thread.sleep(5000); 
        
        Config config = new Config();
        config.useSentinelServers()
            .setLoadBalancer(new RandomLoadBalancer())
            .addSentinelAddress(sentinel3.getRedisServerAddressAndPort()).setMasterName("myMaster");
        
        long t = System.currentTimeMillis();
        RedissonClient redisson = Redisson.create(config);
        assertThat(System.currentTimeMillis() - t).isLessThan(2000L);
        redisson.shutdown();
        
        sentinel1.stop();
        sentinel2.stop();
        sentinel3.stop();
        master.stop();
        slave1.stop();
        slave2.stop();
    }
    
//    @Test
    public void testSentinel() {
        NodesGroup<Node> nodes = redisson.getNodesGroup();
        Assert.assertEquals(5, nodes.getNodes().size());

        nodes.getNodes().stream().forEach((node) -> {
            Assert.assertTrue(node.ping());
        });

        Assert.assertTrue(nodes.pingAll());
    }

    @Test
    public void testClusterConfigJSON() throws IOException {
        Config originalConfig = new Config();
        originalConfig.useClusterServers().addNodeAddress("redis://123.123.1.23:1902", "redis://9.3.1.0:1902");
        String t = originalConfig.toJSON();
        Config c = Config.fromJSON(t);
        assertThat(c.toJSON()).isEqualTo(t);
    }

    @Test
    public void testSingleConfigJSON() throws IOException {
        RedissonClient r = BaseTest.createInstance();
        String t = r.getConfig().toJSON();
        Config c = Config.fromJSON(t);
        assertThat(c.toJSON()).isEqualTo(t);
    }
    
    @Test
    public void testSingleConfigYAML() throws IOException {
        RedissonClient r = BaseTest.createInstance();
        String t = r.getConfig().toYAML();
        Config c = Config.fromYAML(t);
        assertThat(c.toYAML()).isEqualTo(t);
    }

    @Test
    public void testSentinelYAML() throws IOException {
        Config c2 = new Config();
        c2.useSentinelServers().addSentinelAddress("redis://123.1.1.1:1231").setMasterName("mymaster");
        String t = c2.toYAML();
        System.out.println(t);
        Config c = Config.fromYAML(t);
        assertThat(c.toYAML()).isEqualTo(t);
    }

    @Test
    public void testMasterSlaveConfigJSON() throws IOException {
        Config c2 = new Config();
        c2.useMasterSlaveServers().setMasterAddress("redis://123.1.1.1:1231").addSlaveAddress("redis://82.12.47.12:1028");
        String t = c2.toJSON();
        Config c = Config.fromJSON(t);
        assertThat(c.toJSON()).isEqualTo(t);
    }

    @Test
    public void testMasterSlaveConfigYAML() throws IOException {
        Config c2 = new Config();
        c2.useMasterSlaveServers().setMasterAddress("redis://123.1.1.1:1231").addSlaveAddress("redis://82.12.47.12:1028", "redis://82.12.47.14:1028");
        String t = c2.toYAML();
        Config c = Config.fromYAML(t);
        assertThat(c.toYAML()).isEqualTo(t);
    }
    
    @Test
    public void testNodesInCluster() throws Exception {
        RedisRunner master1 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner master2 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner master3 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner slot1 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner slot2 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner slot3 = new RedisRunner().randomPort().randomDir().nosave();
        
        ClusterRunner clusterRunner = new ClusterRunner()
                .addNode(master1, slot1)
                .addNode(master2, slot2)
                .addNode(master3, slot3);
        ClusterProcesses process = clusterRunner.run();
        
        Config config = new Config();
        config.useClusterServers()
        .setLoadBalancer(new RandomLoadBalancer())
        .addNodeAddress(process.getNodes().stream().findAny().get().getRedisServerAddressAndPort());
        RedissonClient redisson = Redisson.create(config);
        
        for (Node node : redisson.getClusterNodesGroup().getNodes()) {
            assertThat(node.info(InfoSection.ALL)).isNotEmpty();
        }
        assertThat(redisson.getClusterNodesGroup().getNodes(NodeType.SLAVE)).hasSize(3);
        assertThat(redisson.getClusterNodesGroup().getNodes(NodeType.MASTER)).hasSize(3);
        assertThat(redisson.getClusterNodesGroup().getNodes()).hasSize(6);
        
        redisson.shutdown();
        process.shutdown();
    }
    
    @Test
    public void testMovedRedirectInCluster() throws Exception {
        RedisRunner master1 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner master2 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner master3 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner slot1 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner slot2 = new RedisRunner().randomPort().randomDir().nosave();
        RedisRunner slot3 = new RedisRunner().randomPort().randomDir().nosave();
        
        ClusterRunner clusterRunner = new ClusterRunner()
                .addNode(master1, slot1)
                .addNode(master2, slot2)
                .addNode(master3, slot3);
        ClusterProcesses process = clusterRunner.run();
        
        Config config = new Config();
        config.useClusterServers()
        .setScanInterval(100000)
        .setLoadBalancer(new RandomLoadBalancer())
        .addNodeAddress(process.getNodes().stream().findAny().get().getRedisServerAddressAndPort());
        RedissonClient redisson = Redisson.create(config);
        
        RedisClientConfig cfg = new RedisClientConfig();
        cfg.setAddress(process.getNodes().iterator().next().getRedisServerAddressAndPort());
        RedisClient c = RedisClient.create(cfg);
        RedisConnection cc = c.connect();
        List<ClusterNodeInfo> cn = cc.sync(RedisCommands.CLUSTER_NODES);
        cn = cn.stream().filter(i -> i.containsFlag(Flag.MASTER)).collect(Collectors.toList());
        Iterator<ClusterNodeInfo> nodesIter = cn.iterator();
        
        
        ClusterNodeInfo source = nodesIter.next();
        ClusterNodeInfo destination = nodesIter.next();

        RedisClientConfig sourceCfg = new RedisClientConfig();
        sourceCfg.setAddress(source.getAddress());
        RedisClient sourceClient = RedisClient.create(sourceCfg);
        RedisConnection sourceConnection = sourceClient.connect();

        RedisClientConfig destinationCfg = new RedisClientConfig();
        destinationCfg.setAddress(destination.getAddress());
        RedisClient destinationClient = RedisClient.create(destinationCfg);
        RedisConnection destinationConnection = destinationClient.connect();
        
        String key = null;
        int slot = 0;
        for (int i = 0; i < 100000; i++) {
            key = "" + i;
            slot = CRC16.crc16(key.getBytes()) % MasterSlaveConnectionManager.MAX_SLOT;
            if (source.getSlotRanges().iterator().next().getStartSlot() == slot) {
                break;
            }
        }
        
        redisson.getBucket(key).set("123");

        destinationConnection.sync(RedisCommands.CLUSTER_SETSLOT, source.getSlotRanges().iterator().next().getStartSlot(), "IMPORTING", source.getNodeId());
        sourceConnection.sync(RedisCommands.CLUSTER_SETSLOT, source.getSlotRanges().iterator().next().getStartSlot(), "MIGRATING", destination.getNodeId());
        
        List<String> keys = sourceConnection.sync(RedisCommands.CLUSTER_GETKEYSINSLOT, source.getSlotRanges().iterator().next().getStartSlot(), 100);
        List<Object> params = new ArrayList<Object>();
        params.add(destination.getAddress().getHost());
        params.add(destination.getAddress().getPort());
        params.add("");
        params.add(0);
        params.add(2000);
        params.add("KEYS");
        params.addAll(keys);
        sourceConnection.async(RedisCommands.MIGRATE, params.toArray());
        
        for (ClusterNodeInfo node : cn) {
            RedisClientConfig cc1 = new RedisClientConfig();
            cc1.setAddress(node.getAddress());
            RedisClient ccc = RedisClient.create(cc1);
            RedisConnection connection = ccc.connect();
            connection.sync(RedisCommands.CLUSTER_SETSLOT, slot, "NODE", destination.getNodeId());
        }
        
        redisson.getBucket(key).set("123");
        redisson.getBucket(key).get();
        
        redisson.shutdown();
        process.shutdown();
    }


    @Test(expected = RedisConnectionException.class)
    public void testSingleConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://127.99.0.1:1111");
        Redisson.create(config);

        Thread.sleep(1500);
    }

    @Test(expected = RedisConnectionException.class)
    public void testClusterConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useClusterServers().addNodeAddress("redis://127.99.0.1:1111");
        Redisson.create(config);

        Thread.sleep(1500);
    }

    @Test(expected = RedisConnectionException.class)
    public void testReplicatedConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useReplicatedServers().addNodeAddress("redis://127.99.0.1:1111");
        Redisson.create(config);

        Thread.sleep(1500);
    }

    @Test(expected = RedisConnectionException.class)
    public void testMasterSlaveConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useMasterSlaveServers().setMasterAddress("redis://127.99.0.1:1111");
        Redisson.create(config);

        Thread.sleep(1500);
    }

    @Test(expected = RedisConnectionException.class)
    public void testSentinelConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useSentinelServers().addSentinelAddress("redis://127.99.0.1:1111").setMasterName("test");
        Redisson.create(config);

        Thread.sleep(1500);
    }

    @Test
    public void testManyConnections() {
        Assume.assumeFalse(RedissonRuntimeEnvironment.isTravis);
        Config redisConfig = new Config();
        redisConfig.useSingleServer()
        .setConnectionMinimumIdleSize(10000)
        .setConnectionPoolSize(10000)
        .setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());
        RedissonClient r = Redisson.create(redisConfig);
        r.shutdown();
    }

    private RedisProcess redisTestSmallMemory() throws IOException, InterruptedException {
        return new RedisRunner()
                .maxmemory("1mb")
                .nosave()
                .randomDir()
                .randomPort()
                .run();
    }

    private RedisProcess redisTestConnection() throws IOException, InterruptedException {
        return new RedisRunner()
                .nosave()
                .randomDir()
                .randomPort()
                .run();
    }
    
}
