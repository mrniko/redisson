package org.redisson;

import static org.assertj.core.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.redisson.client.RedisConnection;
import org.redisson.client.RedisConnectionException;
import org.redisson.client.WriteRedisConnectionException;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.handler.CommandDecoder;
import org.redisson.client.handler.CommandEncoder;
import org.redisson.client.handler.CommandsListEncoder;
import org.redisson.client.handler.CommandsQueue;
import org.redisson.client.handler.ConnectionWatchdog;
import org.redisson.codec.SerializationCodec;
import org.redisson.connection.ConnectionListener;
import org.redisson.core.ClusterNode;
import org.redisson.core.Node;
import org.redisson.core.NodesGroup;
import org.redisson.core.RMap;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.GenericFutureListener;

import static com.jayway.awaitility.Awaitility.*;

public class RedissonTest {

  RedissonClient redisson;

  public static class Dummy {
    private String field;
  }

  @Test(expected = WriteRedisConnectionException.class)
  public void testSer() {
    Config config = new Config();
    config.useSingleServer().setAddress("127.0.0.1:6379");
    config.setCodec(new SerializationCodec());
    RedissonClient r = Redisson.create(config);
    r.getMap("test").put("1", new Dummy());
  }

  @Test
  public void testConnectionListener() throws IOException, InterruptedException, TimeoutException {

    Process p = RedisRunner.runRedis("/redis_connectionListener_test.conf");

    final AtomicInteger connectCounter = new AtomicInteger();
    final AtomicInteger disconnectCounter = new AtomicInteger();

    Config config = new Config();
    config.useSingleServer().setAddress("127.0.0.1:6319").setFailedAttempts(1).setRetryAttempts(1)
        .setConnectionMinimumIdleSize(0);

    RedissonClient r = Redisson.create(config);

    int id = r.getNodesGroup().addConnectionListener(new ConnectionListener() {

      @Override
      public void onDisconnect(InetSocketAddress addr) {
        assertThat(addr).isEqualTo(new InetSocketAddress("127.0.0.1", 6319));
        disconnectCounter.incrementAndGet();
      }

      @Override
      public void onConnect(InetSocketAddress addr) {
        assertThat(addr).isEqualTo(new InetSocketAddress("127.0.0.1", 6319));
        connectCounter.incrementAndGet();
      }
    });

    assertThat(id).isNotZero();

    r.getBucket("1").get();
    p.destroy();
    Assert.assertEquals(1, p.waitFor());

    try {
      r.getBucket("1").get();
    } catch (Exception e) {
    }

    p = RedisRunner.runRedis("/redis_connectionListener_test.conf");

    r.getBucket("1").get();

    r.shutdown();

    p.destroy();
    Assert.assertEquals(1, p.waitFor());

    await().atMost(1, TimeUnit.SECONDS).until(() -> assertThat(connectCounter.get()).isEqualTo(2));
    await().until(() -> assertThat(disconnectCounter.get()).isEqualTo(1));
  }

  @Test
  public void testShutdown() {
    Config config = new Config();
    config.useSingleServer().setAddress("127.0.0.1:6379");

    RedissonClient r = Redisson.create(config);
    Assert.assertFalse(r.isShuttingDown());
    Assert.assertFalse(r.isShutdown());
    r.shutdown();
    Assert.assertTrue(r.isShuttingDown());
    Assert.assertTrue(r.isShutdown());
  }

  // @Test
  public void test() {
    NodesGroup<Node> nodes = redisson.getNodesGroup();
    Assert.assertEquals(1, nodes.getNodes().size());
    Iterator<Node> iter = nodes.getNodes().iterator();

    Node node1 = iter.next();
    Assert.assertTrue(node1.ping());

    Assert.assertTrue(nodes.pingAll());
  }

  // @Test
  public void testSentinel() {
    NodesGroup<Node> nodes = redisson.getNodesGroup();
    Assert.assertEquals(5, nodes.getNodes().size());

    for (Node node : nodes.getNodes()) {
      Assert.assertTrue(node.ping());
    }

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
    RedissonClient r = Redisson.create();
    String t = r.getConfig().toJSON();
    Config c = Config.fromJSON(t);
    assertThat(c.toJSON()).isEqualTo(t);
  }

  @Test
  public void testMasterSlaveConfig() throws IOException {
    Config c2 = new Config();
    c2.useMasterSlaveServers().setMasterAddress("123.1.1.1:1231")
        .addSlaveAddress("82.12.47.12:1028");

    String t = c2.toJSON();
    Config c = Config.fromJSON(t);
    assertThat(c.toJSON()).isEqualTo(t);
  }

  @Test
  public void testCluster() {
    NodesGroup<ClusterNode> nodes = redisson.getClusterNodesGroup();
    Assert.assertEquals(2, nodes.getNodes().size());

    for (ClusterNode node : nodes.getNodes()) {
      Map<String, String> params = node.info();
      Assert.assertNotNull(params);
      Assert.assertTrue(node.ping());
    }

    Assert.assertTrue(nodes.pingAll());
  }

  @Test(expected = RedisConnectionException.class)
  public void testSingleConnectionFail() throws InterruptedException {
    Config config = new Config();
    config.useSingleServer().setAddress("127.0.0.1:1111");
    Redisson.create(config);

    Thread.sleep(1500);
  }

  @Test(expected = RedisConnectionException.class)
  public void testClusterConnectionFail() throws InterruptedException {
    Config config = new Config();
    config.useClusterServers().addNodeAddress("127.0.0.1:1111");
    Redisson.create(config);

    Thread.sleep(1500);
  }

  @Test(expected = RedisConnectionException.class)
  public void testElasticacheConnectionFail() throws InterruptedException {
    Config config = new Config();
    config.useElasticacheServers().addNodeAddress("127.0.0.1:1111");
    Redisson.create(config);

    Thread.sleep(1500);
  }

  @Test(expected = RedisConnectionException.class)
  public void testMasterSlaveConnectionFail() throws InterruptedException {
    Config config = new Config();
    config.useMasterSlaveServers().setMasterAddress("127.0.0.1:1111");
    Redisson.create(config);

    Thread.sleep(1500);
  }

  @Test(expected = RedisConnectionException.class)
  public void testSentinelConnectionFail() throws InterruptedException {
    Config config = new Config();
    config.useSentinelServers().addSentinelAddress("127.0.0.1:1111");
    Redisson.create(config);

    Thread.sleep(1500);
  }

  @Test
  public void testManyConnections() {
    Config redisConfig = new Config();
    redisConfig.useSingleServer().setConnectionMinimumIdleSize(10000).setConnectionPoolSize(10000)
        .setAddress("localhost:6379");
    RedissonClient r = Redisson.create(redisConfig);
    r.shutdown();
  }


}
