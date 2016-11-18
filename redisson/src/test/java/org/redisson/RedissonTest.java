package org.redisson;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.redisson.RedisRunner.RedisProcess;
import org.redisson.api.ClusterNode;
import org.redisson.api.Node;
import org.redisson.api.NodesGroup;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.RedisException;
import org.redisson.client.RedisOutOfMemoryException;
import org.redisson.client.protocol.decoder.ListScanResult;
import org.redisson.codec.SerializationCodec;
import org.redisson.config.Config;
import org.redisson.connection.ConnectionListener;

public class RedissonTest extends AbstractBaseTest {

    @Test
    public void testIterator() {
        RedissonBaseIterator iter = new RedissonBaseIterator() {
            int i;
            @Override
            ListScanResult iterator(InetSocketAddress client, long nextIterPos) {
                i++;
                if (i == 1) {
                    return new ListScanResult(13L, Collections.emptyList());
                }
                if (i == 2) {
                    return new ListScanResult(0L, Collections.emptyList());
                }
                Assert.fail();
                return null;
            }

            @Override
            void remove(Object value) {
            }
            
        };
        
        Assert.assertFalse(iter.hasNext());
    }
    
    public static class Dummy {
        private String field;
    }

    @Test(expected = RedisException.class)
    public void testSer() {
        Config config = new Config();
        config.useSingleServer().setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());
        config.setCodec(new SerializationCodec());
        RedissonClient r = redissonRule.createClient(config);
        r.getMap("test").put("1", new Dummy());
    }
    
    @Test(expected = RedisOutOfMemoryException.class)
    public void testMemoryScript() throws IOException, InterruptedException {
        RedisProcess p = redisTestSmallMemory();

        Config config = new Config();
        config.useSingleServer().setAddress(p.getRedisServerAddressAndPort()).setTimeout(100000);

        try {
            RedissonClient r = redissonRule.createClient(config);
            r.getKeys().flushall();
            for (int i = 0; i < 10000; i++) {
                r.getMap("test").put("" + i, "" + i);
            }
        } finally {
            p.stop();
        }
    }

    @Test(expected = RedisOutOfMemoryException.class)
    public void testMemoryCommand() throws IOException, InterruptedException {
        RedisProcess p = redisTestSmallMemory();

        Config config = new Config();
        config.useSingleServer().setAddress(p.getRedisServerAddressAndPort()).setTimeout(100000);

        try {
            RedissonClient r = redissonRule.createClient(config);
            r.getKeys().flushall();
            for (int i = 0; i < 10000; i++) {
                r.getMap("test").fastPut("" + i, "" + i);
            }
        } finally {
            p.stop();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigValidation() {
        Config redissonConfig = new Config();
        redissonConfig.useSingleServer()
        .setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort())
        .setConnectionPoolSize(2);
        redissonRule.createClient(redissonConfig);        
    }
    
    @Test
    public void testConnectionListener() throws IOException, InterruptedException, TimeoutException {

        final RedisProcess p = redisTestConnection();

        final AtomicInteger connectCounter = new AtomicInteger();
        final AtomicInteger disconnectCounter = new AtomicInteger();

        Config config = new Config();
        config.useSingleServer().setAddress(p.getRedisServerAddressAndPort());

        RedissonClient r = redissonRule.createClient(config);

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
        
        try {
            r.getBucket("1").get();
        } catch (Exception e) {
        }

        RedisProcess pp = new RedisRunner()
                .nosave()
                .port(p.getRedisServerPort())
                .randomDir()
                .run();

        r.getBucket("1").get();

        Assert.assertEquals(0, pp.stop());

        await().atMost(1, TimeUnit.SECONDS).until(() -> assertThat(connectCounter.get()).isEqualTo(1));
        await().until(() -> assertThat(disconnectCounter.get()).isEqualTo(1));
    }

    @Test
    public void testShutdown() {
        Config config = new Config();
        config.useSingleServer().setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());

        RedissonClient r = redissonRule.createClient(config);
        Assert.assertFalse(r.isShuttingDown());
        Assert.assertFalse(r.isShutdown());
        r.shutdown();
        Assert.assertTrue(r.isShuttingDown());
        Assert.assertTrue(r.isShutdown());
    }

    @Test
    public void testTime() {
        NodesGroup<Node> nodes = redissonRule.getSharedClient().getNodesGroup();
        Assert.assertEquals(1, nodes.getNodes().size());
        Iterator<Node> iter = nodes.getNodes().iterator();

        Node node1 = iter.next();
        assertThat(node1.time()).isGreaterThan(100000L);
    }

    @Test
    public void testPing() {
        NodesGroup<Node> nodes = redissonRule.getSharedClient().getNodesGroup();
        Assert.assertEquals(1, nodes.getNodes().size());
        Iterator<Node> iter = nodes.getNodes().iterator();

        Node node1 = iter.next();
        Assert.assertTrue(node1.ping());

        Assert.assertTrue(nodes.pingAll());
    }

//    @Test
    public void testSentinel() {
        NodesGroup<Node> nodes = redissonRule.getSharedClient().getNodesGroup();
        Assert.assertEquals(5, nodes.getNodes().size());

        nodes.getNodes().stream().forEach((node) -> {
            Assert.assertTrue(node.ping());
        });

        Assert.assertTrue(nodes.pingAll());
    }

    @Test
    public void testClusterConfig() throws IOException {
        Config originalConfig = new Config();
        originalConfig.useClusterServers().addNodeAddress("123.123.1.23:1902", "9.3.1.0:1902");
        String t = originalConfig.toJSON();
        Config c = Config.fromJSON(t);
        System.out.println(t);
        assertThat(c.toJSON()).isEqualTo(t);
    }

    @Test
    public void testSingleConfig() throws IOException {
        RedissonClient r = redissonRule.getSharedClient();
        String t = r.getConfig().toJSON();
        Config c = Config.fromJSON(t);
        assertThat(c.toJSON()).isEqualTo(t);
    }

    @Test
    public void testMasterSlaveConfig() throws IOException {
        Config c2 = new Config();
        c2.useMasterSlaveServers().setMasterAddress("123.1.1.1:1231").addSlaveAddress("82.12.47.12:1028");

        String t = c2.toJSON();
        Config c = Config.fromJSON(t);
        assertThat(c.toJSON()).isEqualTo(t);
    }

//    @Test
    public void testCluster() {
        NodesGroup<ClusterNode> nodes = redissonRule.getSharedClient().getClusterNodesGroup();
        Assert.assertEquals(2, nodes.getNodes().size());

        nodes.getNodes().stream().forEach((node) -> {
            Map<String, String> params = node.info();
            Assert.assertNotNull(params);
            Assert.assertTrue(node.ping());
        });

        Assert.assertTrue(nodes.pingAll());
    }

    @Test(expected = RedisConnectionException.class)
    public void testSingleConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useSingleServer().setAddress("127.99.0.1:1111");
        redissonRule.createClient(config);

        Thread.sleep(1500);
    }

    @Test(expected = RedisConnectionException.class)
    public void testClusterConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useClusterServers().addNodeAddress("127.99.0.1:1111");
        redissonRule.createClient(config);

        Thread.sleep(1500);
    }

    @Test(expected = RedisConnectionException.class)
    public void testElasticacheConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useElasticacheServers().addNodeAddress("127.99.0.1:1111");
        redissonRule.createClient(config);

        Thread.sleep(1500);
    }

    @Test(expected = RedisConnectionException.class)
    public void testMasterSlaveConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useMasterSlaveServers().setMasterAddress("127.99.0.1:1111");
        redissonRule.createClient(config);

        Thread.sleep(1500);
    }

    @Test(expected = RedisConnectionException.class)
    public void testSentinelConnectionFail() throws InterruptedException {
        Config config = new Config();
        config.useSentinelServers().addSentinelAddress("127.99.0.1:1111");
        redissonRule.createClient(config);

        Thread.sleep(1500);
    }

    @Test
    public void testManyConnections() {
        Assume.assumeFalse(RedissonRuntimeEnvironment.isTravis);
        Config redisConfig = new Config();
        redisConfig.useSingleServer()
        .setConnectionMinimumIdleSize(9000)
        .setConnectionPoolSize(9000)
        .setAddress(RedisRunner.getDefaultRedisServerBindAddressAndPort());
        RedissonClient r = redissonRule.createClient(redisConfig);
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
