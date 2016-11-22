package org.redisson;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.redisson.api.RFuture;
import org.redisson.api.RScript;
import org.redisson.api.RScript.Mode;
import org.redisson.client.RedisException;

public class RedissonScriptTest extends AbstractBaseTest {

    @Test
    public void testEval() {
        RScript script = redissonRule.getSharedClient().getScript();
        List<Object> res = script.eval(RScript.Mode.READ_ONLY, "return {1,2,3.3333,'\"foo\"',nil,'bar'}", RScript.ReturnType.MULTI, Collections.emptyList());
        assertThat(res).containsExactly(1L, 2L, 3L, "foo");
    }

    @Test
    public void testEvalAsync() {
        RScript script = redissonRule.getSharedClient().getScript();
        RFuture<List<Object>> res = script.evalAsync(RScript.Mode.READ_ONLY, "return {1,2,3.3333,'\"foo\"',nil,'bar'}", RScript.ReturnType.MULTI, Collections.emptyList());
        assertThat(res.awaitUninterruptibly().getNow()).containsExactly(1L, 2L, 3L, "foo");
    }

    @Test
    public void testScriptExists() {
        RScript s = redissonRule.getSharedClient().getScript();
        String r = s.scriptLoad("return redis.call('get', 'foo')");
        Assert.assertEquals("282297a0228f48cd3fc6a55de6316f31422f5d17", r);

        List<Boolean> r1 = s.scriptExists(r);
        Assert.assertEquals(1, r1.size());
        Assert.assertTrue(r1.get(0));

        s.scriptFlush();

        List<Boolean> r2 = s.scriptExists(r);
        Assert.assertEquals(1, r2.size());
        Assert.assertFalse(r2.get(0));
    }

    @Test
    public void testScriptFlush() {
        redissonRule.getSharedClient().getBucket("foo").set("bar");
        String r = redissonRule.getSharedClient().getScript().scriptLoad("return redis.call('get', 'foo')");
        Assert.assertEquals("282297a0228f48cd3fc6a55de6316f31422f5d17", r);
        String r1 = redissonRule.getSharedClient().getScript().evalSha(Mode.READ_ONLY, "282297a0228f48cd3fc6a55de6316f31422f5d17", RScript.ReturnType.VALUE, Collections.emptyList());
        Assert.assertEquals("bar", r1);
        redissonRule.getSharedClient().getScript().scriptFlush();

        try {
            redissonRule.getSharedClient().getScript().evalSha(Mode.READ_ONLY, "282297a0228f48cd3fc6a55de6316f31422f5d17", RScript.ReturnType.VALUE, Collections.emptyList());
        } catch (Exception e) {
            Assert.assertEquals(RedisException.class, e.getClass());
        }
    }

    @Test
    public void testScriptLoad() {
        redissonRule.getSharedClient().getBucket("foo").set("bar");
        String r = redissonRule.getSharedClient().getScript().scriptLoad("return redis.call('get', 'foo')");
        Assert.assertEquals("282297a0228f48cd3fc6a55de6316f31422f5d17", r);
        String r1 = redissonRule.getSharedClient().getScript().evalSha(Mode.READ_ONLY, "282297a0228f48cd3fc6a55de6316f31422f5d17", RScript.ReturnType.VALUE, Collections.emptyList());
        Assert.assertEquals("bar", r1);
    }

    @Test
    public void testScriptLoadAsync() {
        redissonRule.getSharedClient().getBucket("foo").set("bar");
        RFuture<String> r = redissonRule.getSharedClient().getScript().scriptLoadAsync("return redis.call('get', 'foo')");
        Assert.assertEquals("282297a0228f48cd3fc6a55de6316f31422f5d17", r.awaitUninterruptibly().getNow());
        String r1 = redissonRule.getSharedClient().getScript().evalSha(Mode.READ_ONLY, "282297a0228f48cd3fc6a55de6316f31422f5d17", RScript.ReturnType.VALUE, Collections.emptyList());
        Assert.assertEquals("bar", r1);
    }

    @Test
    public void testEvalSha() {
        RScript s = redissonRule.getSharedClient().getScript();
        String res = s.scriptLoad("return redis.call('get', 'foo')");
        Assert.assertEquals("282297a0228f48cd3fc6a55de6316f31422f5d17", res);

        redissonRule.getSharedClient().getBucket("foo").set("bar");
        String r1 = s.evalSha(Mode.READ_ONLY, "282297a0228f48cd3fc6a55de6316f31422f5d17", RScript.ReturnType.VALUE, Collections.emptyList());
        Assert.assertEquals("bar", r1);
    }

    @Test
    public void testEvalshaAsync() {
        RScript s = redissonRule.getSharedClient().getScript();
        String res = s.scriptLoad("return redis.call('get', 'foo')");
        Assert.assertEquals("282297a0228f48cd3fc6a55de6316f31422f5d17", res);

        redissonRule.getSharedClient().getBucket("foo").set("bar");
        String r = redissonRule.getSharedClient().getScript().eval(Mode.READ_ONLY, "return redis.call('get', 'foo')", RScript.ReturnType.VALUE);
        Assert.assertEquals("bar", r);
        RFuture<Object> r1 = redissonRule.getSharedClient().getScript().evalShaAsync(Mode.READ_ONLY, "282297a0228f48cd3fc6a55de6316f31422f5d17", RScript.ReturnType.VALUE, Collections.emptyList());
        Assert.assertEquals("bar", r1.awaitUninterruptibly().getNow());
    }
}
