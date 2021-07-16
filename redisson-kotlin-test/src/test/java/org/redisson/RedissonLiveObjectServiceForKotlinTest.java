package org.redisson;

import org.junit.Test;
import org.redisson.api.RBlockingDeque;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RDeque;
import org.redisson.api.RList;
import org.redisson.api.RLiveObject;
import org.redisson.api.RLiveObjectService;
import org.redisson.api.RMap;
import org.redisson.api.RObject;
import org.redisson.api.RQueue;
import org.redisson.api.RSet;
import org.redisson.api.RSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.api.condition.Conditions;
import org.redisson.config.Config;
import org.redisson.liveobject.resolver.DefaultNamingScheme;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.redisson.kotlin.KotlinEntities.ClassWithoutIdSetterGetter;
import static org.redisson.kotlin.KotlinEntities.Customer;
import static org.redisson.kotlin.KotlinEntities.Dog;
import static org.redisson.kotlin.KotlinEntities.HasIsAccessor;
import static org.redisson.kotlin.KotlinEntities.MyCustomer;
import static org.redisson.kotlin.KotlinEntities.MyObject;
import static org.redisson.kotlin.KotlinEntities.MyObjectWithList;
import static org.redisson.kotlin.KotlinEntities.ObjectId;
import static org.redisson.kotlin.KotlinEntities.ObjectWithList;
import static org.redisson.kotlin.KotlinEntities.Order;
import static org.redisson.kotlin.KotlinEntities.SetterEncapsulation;
import static org.redisson.kotlin.KotlinEntities.SimpleObject;
import static org.redisson.kotlin.KotlinEntities.TestClass;
import static org.redisson.kotlin.KotlinEntities.TestClassID1;
import static org.redisson.kotlin.KotlinEntities.TestClassID2;
import static org.redisson.kotlin.KotlinEntities.TestClassNoTransformation;
import static org.redisson.kotlin.KotlinEntities.TestIndexed;
import static org.redisson.kotlin.KotlinEntities.TestIndexed1;
import static org.redisson.kotlin.KotlinEntities.TestREntity;
import static org.redisson.kotlin.KotlinEntities.TestREntityIdNested;
import static org.redisson.kotlin.KotlinEntities.TestREntityValueNested;
import static org.redisson.kotlin.KotlinEntities.TestREntityWithMap;
import static org.redisson.kotlin.KotlinEntities.TestREntityWithRMap;
import static org.redisson.kotlin.KotlinEntities.TestEnum;

public class RedissonLiveObjectServiceForKotlinTest extends BaseTest {

	@Test
	public void testFindEq2() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setNum1(1);
		t1.setName1("common");
		t1.setName2(";asdlkfj");
		t1 = s.persist(t1);

		TestIndexed t2 = new TestIndexed("2");
		t2.setNum1(1);
		t2.setName1("common");
		t2.setName2("893123");
		t2 = s.persist(t2);

		TestIndexed t3 = new TestIndexed("3");
		t3.setNum1(1);
		t3.setName1("common");
		t3.setName2("hkf;glhsdfg");
		t3 = s.persist(t3);

		Collection<TestIndexed> objects2 = s.find(TestIndexed.class, Conditions.and(
				Conditions.eq("num1", 1),
				Conditions.eq("name1", "jkflasdf"),
				Conditions.eq("name2", "fdfdf")));
		assertThat(objects2).isEmpty();

		Collection<TestIndexed> objects1 = s.find(TestIndexed.class, Conditions.and(
				Conditions.eq("num1", 1),
				Conditions.eq("name1", "common"),
				Conditions.eq("name2", "hkf;glhsdfg")));
		assertThat(objects1).hasSize(1);
	}

	@Test
	public void testFindLe() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setNum1(12);
		t1 = s.persist(t1);

		TestIndexed t2 = new TestIndexed("2");
		t2.setNum1(10);
		t2 = s.persist(t2);

		Collection<TestIndexed> objects2 = s.find(TestIndexed.class, Conditions.le("num1", 9));
		assertThat(objects2).isEmpty();

		Collection<TestIndexed> objects0 = s.find(TestIndexed.class, Conditions.le("num1", 12));
		assertThat(objects0).hasSize(2);
		Iterator<TestIndexed> iter = objects0.iterator();
		TestIndexed obj1 = iter.next();
		assertThat(obj1.getId()).isEqualTo(t1.getId());
		TestIndexed obj2 = iter.next();
		assertThat(obj2.getId()).isEqualTo(t2.getId());

		s.delete(t1);
		s.delete(t2);

		Collection<TestIndexed> objects3 = s.find(TestIndexed.class, Conditions.le("num1", 12));
		assertThat(objects3).isEmpty();

		TestIndexed t3 = new TestIndexed("3");
		t3.setName1("test31");
		t3.setNum1(32);
		t3.setBool1(false);
		t3 = s.persist(t3);

		TestIndexed t4 = new TestIndexed("4");
		t4 = s.persist(t4);
		t4.setName1("test41");
		t4.setNum1(42);
		t4.setBool1(true);

		Collection<TestIndexed> objects4 = s.find(TestIndexed.class, Conditions.or(Conditions.le("num1", 30), Conditions.le("num1", 32)));
		assertThat(objects4).hasSize(1);

		Collection<TestIndexed> objects41 = s.find(TestIndexed.class, Conditions.or(Conditions.le("num1", 31), Conditions.lt("num1", -1)));
		assertThat(objects41).hasSize(0);

		Collection<TestIndexed> objects5 = s.find(TestIndexed.class, Conditions.or(Conditions.and(Conditions.eq("name1", "test31"), Conditions.le("num1", 32)),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.le("num1", 42))));
		assertThat(objects5).hasSize(2);

		Collection<TestIndexed> objects6 = s.find(TestIndexed.class, Conditions.or(Conditions.eq("name1", "test34"),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.le("num1", 42))));
		assertThat(objects6.iterator().next().getId()).isEqualTo("4");
	}

	@Test
	public void testFindLt() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setNum1(12);
		t1 = s.persist(t1);

		TestIndexed t2 = new TestIndexed("2");
		t2.setNum1(10);
		t2 = s.persist(t2);

		Collection<TestIndexed> objects2 = s.find(TestIndexed.class, Conditions.lt("num1", 9));
		assertThat(objects2).isEmpty();

		Collection<TestIndexed> objects0 = s.find(TestIndexed.class, Conditions.lt("num1", 13));
		assertThat(objects0).hasSize(2);
		Iterator<TestIndexed> iter = objects0.iterator();
		TestIndexed obj1 = iter.next();
		assertThat(obj1.getId()).isEqualTo(t1.getId());
		TestIndexed obj2 = iter.next();
		assertThat(obj2.getId()).isEqualTo(t2.getId());

		s.delete(t1);
		s.delete(t2);

		Collection<TestIndexed> objects3 = s.find(TestIndexed.class, Conditions.lt("num1", 13));
		assertThat(objects3).isEmpty();

		TestIndexed t3 = new TestIndexed("3");
		t3.setName1("test31");
		t3.setNum1(32);
		t3.setBool1(false);
		t3 = s.persist(t3);

		TestIndexed t4 = new TestIndexed("4");
		t4 = s.persist(t4);
		t4.setName1("test41");
		t4.setNum1(42);
		t4.setBool1(true);

		Collection<TestIndexed> objects4 = s.find(TestIndexed.class, Conditions.or(Conditions.lt("num1", 30), Conditions.lt("num1", 33)));
		assertThat(objects4).hasSize(1);

		Collection<TestIndexed> objects41 = s.find(TestIndexed.class, Conditions.or(Conditions.lt("num1", 32), Conditions.lt("num1", -1)));
		assertThat(objects41).hasSize(0);

		Collection<TestIndexed> objects5 = s.find(TestIndexed.class, Conditions.or(Conditions.and(Conditions.eq("name1", "test31"), Conditions.lt("num1", 33)),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.lt("num1", 43))));
		assertThat(objects5).hasSize(2);

		Collection<TestIndexed> objects6 = s.find(TestIndexed.class, Conditions.or(Conditions.eq("name1", "test34"),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.lt("num1", 43))));
		assertThat(objects6.iterator().next().getId()).isEqualTo("4");
	}

	@Test
	public void testFindGe() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setNum1(12);
		t1 = s.persist(t1);

		TestIndexed t2 = new TestIndexed("2");
		t2.setNum1(10);
		t2 = s.persist(t2);

		Collection<TestIndexed> objects0 = s.find(TestIndexed.class, Conditions.ge("num1", 10));
		assertThat(objects0).hasSize(2);
		Iterator<TestIndexed> iter = objects0.iterator();
		TestIndexed obj1 = iter.next();
		assertThat(obj1.getId()).isEqualTo(t1.getId());
		TestIndexed obj2 = iter.next();
		assertThat(obj2.getId()).isEqualTo(t2.getId());

		s.delete(t1);
		s.delete(t2);

		Collection<TestIndexed> objects3 = s.find(TestIndexed.class, Conditions.ge("num1", 10));
		assertThat(objects3).isEmpty();

		TestIndexed t3 = new TestIndexed("3");
		t3.setName1("test31");
		t3.setNum1(32);
		t3.setBool1(false);
		t3 = s.persist(t3);

		TestIndexed t4 = new TestIndexed("4");
		t4 = s.persist(t4);
		t4.setName1("test41");
		t4.setNum1(42);
		t4.setBool1(true);

		Collection<TestIndexed> objects4 = s.find(TestIndexed.class, Conditions.or(Conditions.ge("num1", 42), Conditions.ge("num1", 43)));
		assertThat(objects4).hasSize(1);

		Collection<TestIndexed> objects41 = s.find(TestIndexed.class, Conditions.or(Conditions.ge("num1", 43), Conditions.ge("num1", 45)));
		assertThat(objects41).hasSize(0);

		Collection<TestIndexed> objects5 = s.find(TestIndexed.class, Conditions.or(Conditions.and(Conditions.eq("name1", "test31"), Conditions.ge("num1", 32)),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.ge("num1", 42))));
		assertThat(objects5).hasSize(2);

		Collection<TestIndexed> objects6 = s.find(TestIndexed.class, Conditions.or(Conditions.eq("name1", "test34"),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.ge("num1", 42))));
		assertThat(objects6.iterator().next().getId()).isEqualTo("4");
	}

	@Test
	public void testFindGt() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setNum1(12);
		t1 = s.persist(t1);

		TestIndexed t2 = new TestIndexed("2");
		t2.setNum1(10);
		t2 = s.persist(t2);

		Collection<TestIndexed> objects0 = s.find(TestIndexed.class, Conditions.gt("num1", 9));
		assertThat(objects0).hasSize(2);
		Iterator<TestIndexed> iter = objects0.iterator();
		TestIndexed obj1 = iter.next();
		assertThat(obj1.getId()).isEqualTo(t1.getId());
		TestIndexed obj2 = iter.next();
		assertThat(obj2.getId()).isEqualTo(t2.getId());

		s.delete(t1);
		s.delete(t2);

		Collection<TestIndexed> objects3 = s.find(TestIndexed.class, Conditions.gt("num1", 9));
		assertThat(objects3).isEmpty();

		TestIndexed t3 = new TestIndexed("3");
		t3.setName1("test31");
		t3.setNum1(32);
		t3.setBool1(false);
		t3 = s.persist(t3);

		TestIndexed t4 = new TestIndexed("4");
		t4 = s.persist(t4);
		t4.setName1("test41");
		t4.setNum1(42);
		t4.setBool1(true);

		Collection<TestIndexed> objects4 = s.find(TestIndexed.class, Conditions.or(Conditions.gt("num1", 40), Conditions.gt("num1", 42)));
		assertThat(objects4).hasSize(1);

		Collection<TestIndexed> objects41 = s.find(TestIndexed.class, Conditions.or(Conditions.gt("num1", 42), Conditions.gt("num1", 45)));
		assertThat(objects41).hasSize(0);

		Collection<TestIndexed> objects5 = s.find(TestIndexed.class, Conditions.or(Conditions.and(Conditions.eq("name1", "test31"), Conditions.gt("num1", 30)),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.gt("num1", 40))));
		assertThat(objects5).hasSize(2);

		Collection<TestIndexed> objects6 = s.find(TestIndexed.class, Conditions.or(Conditions.eq("name1", "test34"),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.gt("num1", 41))));
		assertThat(objects6.iterator().next().getId()).isEqualTo("4");
	}

	@Test
	public void testCountEq() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setName1("test1");
		t1.setNum1(100);
		t1 = s.persist(t1);

		TestIndexed t2 = new TestIndexed("2");
		t2 = s.persist(t2);
		t2.setName1("test1");
		t2.setObj(t1);
		t2.setNum1(100);

		long ids = s.count(TestIndexed.class, Conditions.eq("name1", "test1"));
		assertThat(ids).isEqualTo(2);

		long ids2 = s.count(TestIndexed.class, Conditions.eq("name1", "test2"));
		assertThat(ids2).isZero();

		long ids3 = s.count(TestIndexed.class, Conditions.eq("num1", 100));
		assertThat(ids3).isEqualTo(2);
	}

	@Test
	public void testIndexUpdateCluster() throws IOException, InterruptedException {
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
		ClusterRunner.ClusterProcesses process = clusterRunner.run();

		Config config = new Config();
		config.useClusterServers()
				.addNodeAddress(process.getNodes().stream().findAny().get().getRedisServerAddressAndPort());
		RedissonClient redisson = Redisson.create(config);

		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setName1("test1");
		t1 = s.persist(t1);

		Collection<TestIndexed> objects0 = s.find(TestIndexed.class, Conditions.eq("name1", "test1"));
		assertThat(objects0.iterator().next().getId()).isEqualTo(t1.getId());

		redisson.shutdown();
		process.shutdown();
	}

	@Test
	public void testIndexUpdate() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setName1("test1");
		t1 = s.persist(t1);

		Collection<TestIndexed> objects0 = s.find(TestIndexed.class, Conditions.eq("name1", "test1"));
		assertThat(objects0.iterator().next().getId()).isEqualTo(t1.getId());

		t1.setName1("test2");

		Collection<TestIndexed> objects2 = s.find(TestIndexed.class, Conditions.eq("name1", "test1"));
		assertThat(objects2.isEmpty()).isTrue();
		Collection<TestIndexed> objects3 = s.find(TestIndexed.class, Conditions.eq("name1", "test2"));
		assertThat(objects3.iterator().next().getId()).isEqualTo(t1.getId());
	}

	@Test
	public void testIndexUpdatePrimitive() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setNum2(11);
		t1 = s.persist(t1);

		Collection<TestIndexed> objects0 = s.find(TestIndexed.class, Conditions.eq("num2", 11));
		assertThat(objects0.iterator().next().getId()).isEqualTo(t1.getId());

		t1.setNum2(12);

		Collection<TestIndexed> objects01 = s.find(TestIndexed.class, Conditions.eq("num2", 11));
		assertThat(objects01).isEmpty();
		Collection<TestIndexed> objects1 = s.find(TestIndexed.class, Conditions.eq("num2", 12));
		assertThat(objects1.iterator().next().getId()).isEqualTo(t1.getId());
	}

	@Test
	public void testIndexUpdate2() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setName1("test1");
		t1 = s.persist(t1);

		TestIndexed t2 = new TestIndexed("2");
		t2.setName1("test2");
		t2 = s.persist(t2);
		t1.setObj(t2);

		TestIndexed t3 = new TestIndexed("3");
		t3.setName1("test3");
		t3 = s.persist(t3);
		t1.setObj(t3);
	}

	@Test
	public void testFindEq() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed t1 = new TestIndexed("1");
		t1.setName1("test1");
		t1 = s.persist(t1);

		TestIndexed t2 = new TestIndexed("2");
		t2 = s.persist(t2);
		t2.setName1("test1");
		t2.setObj(t1);

		Collection<TestIndexed> objects0 = s.find(TestIndexed.class, Conditions.eq("obj", t1.getId()));
		assertThat(objects0.iterator().next().getId()).isEqualTo(t2.getId());

		t2.setObj(null);
		Collection<TestIndexed> objects01 = s.find(TestIndexed.class, Conditions.eq("obj", t1.getId()));
		assertThat(objects01).isEmpty();

		Collection<TestIndexed> objects1 = s.find(TestIndexed.class, Conditions.eq("name1", "test1"));
		assertThat(objects1).hasSize(2);

		Collection<TestIndexed> objects2 = s.find(TestIndexed.class, Conditions.eq("name3", "test2"));
		assertThat(objects2).isEmpty();

		s.delete(t1);
		s.delete(t2);

		Collection<TestIndexed> objects3 = s.find(TestIndexed.class, Conditions.eq("name1", "test1"));
		assertThat(objects3).isEmpty();

		TestIndexed t3 = new TestIndexed("3");
		t3.setName1("test31");
		t3.setNum1(32);
		t3.setBool1(false);
		t3 = s.persist(t3);

		TestIndexed t4 = new TestIndexed("4");
		t4 = s.persist(t4);
		t4.setName1("test41");
		t4.setNum1(42);
		t4.setBool1(true);

		Collection<TestIndexed> objects4 = s.find(TestIndexed.class, Conditions.or(Conditions.eq("name1", "test31"), Conditions.eq("name1", "test41")));
		assertThat(objects4).hasSize(2);

		Collection<TestIndexed> objects41 = s.find(TestIndexed.class, Conditions.in("name1", "test31", "test41"));
		assertThat(objects41).hasSize(2);

		Collection<TestIndexed> objects5 = s.find(TestIndexed.class, Conditions.or(Conditions.and(Conditions.eq("name1", "test31"), Conditions.eq("num1", 32)),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.eq("num1", 42))));
		assertThat(objects5).hasSize(2);

		Collection<TestIndexed> objects6 = s.find(TestIndexed.class, Conditions.or(Conditions.eq("name1", "test34"),
				Conditions.and(Conditions.eq("name1", "test41"), Conditions.eq("num1", 42))));
		assertThat(objects6.iterator().next().getId()).isEqualTo("4");

		Collection<TestIndexed> objects7 = s.find(TestIndexed.class, Conditions.eq("bool1", true));
		assertThat(objects7.iterator().next().getId()).isEqualTo("4");

		Collection<TestIndexed> objects8 = s.find(TestIndexed.class, Conditions.and(Conditions.in("name1", "test31", "test30"),
				Conditions.eq("bool1", true)));
		assertThat(objects8).isEmpty();
	}

	@Test
	public void testBasics() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestREntity t = new TestREntity("1", null);
		t = s.persist(t);
		assertEquals("1", t.getName());

		DefaultNamingScheme scheme = new DefaultNamingScheme(redisson.getConfig().getCodec());
		assertTrue(redisson.getMap(scheme.getName(TestREntity.class, "1")).isExists());
		t.setName("3333");

		assertEquals("3333", t.getName());
		assertTrue(redisson.getMap(scheme.getName(TestREntity.class, "3333")).isExists());
		t.setValue("111");
		assertEquals("111", t.getValue());
		assertTrue(redisson.getMap(scheme.getName(TestREntity.class, "3333")).isExists());
		assertTrue(!redisson.getMap(scheme.getName(TestREntity.class, "1")).isExists());
		assertEquals("111", redisson.getMap(scheme.getName(TestREntity.class, "3333")).get("value"));

//        ((RLiveObject) t).getLiveObjectLiveMap().put("value", "555");
//        assertEquals("555", redisson.getMap(REntity.DefaultNamingScheme.INSTANCE.getName(TestREntity.class, "name", "3333")).get("value"));
//        assertEquals("3333", ((RObject) t).getName());//field access takes priority over the implemented interface.
	}

	@Test
	public void testLiveObjectWithCollection() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestREntityWithMap t = new TestREntityWithMap("2", null);
		t = s.persist(t);
		RMap<String, String> map = redisson.<String, String>getMap("testMap");
		t.setValue(map);
		map.put("field", "123");

		TestREntityWithMap t2 = s.get(TestREntityWithMap.class, "2");

		assertEquals("123", t2.getValue().get("field"));

		TestREntityWithMap t3 = s.get(TestREntityWithMap.class, "2");
		Map<String, String> map1 = (Map<String, String>) t3.getValue();
		if (map1 != null) {
			map1.put("field", "333");
		}

		t3 = s.get(TestREntityWithMap.class, "2");
		assertEquals("333", t3.getValue().get("field"));

		HashMap<String, String> map2 = new HashMap<>();
		map2.put("field", "hello");
		t.setValue(map2);

		t3 = s.get(TestREntityWithMap.class, "2");
		assertEquals("hello", t3.getValue().get("field"));
	}

	@Test
	public void testLiveObjectWithRObject() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestREntityWithRMap t = new TestREntityWithRMap("2", null);
		t = s.persist(t);
		RMap<String, String> map = redisson.<String, String>getMap("testMap");
		t.setValue(map);
		map.put("field", "123");
		assertEquals("123",
				s.<TestREntityWithRMap>get(TestREntityWithRMap.class, "2")
						.getValue().get("field"));
		t = s.get(TestREntityWithRMap.class, "2");
		Map<String, String> map1 = (Map<String, String>) t.getValue();
		if (map1 != null) {
			map1.put("field", "333");
		}
		assertEquals("333",
				s.<TestREntityWithRMap>get(TestREntityWithRMap.class, "2")
						.getValue().get("field"));
	}

	@Test
	public void testLiveObjectWithNestedLiveObjectAsId() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestREntity t1 = new TestREntity("1", null);
		t1 = s.persist(t1);

		try {
			s.persist(new TestREntityIdNested(t1));
			fail("Should not be here");
		} catch (Exception e) {
			assertEquals("Field with RId annotation cannot be a type of which class is annotated with REntity.", e.getMessage());
		}
	}

	@Test
	public void testLiveObjectWithNestedLiveObjectAsValue() throws Exception {
		RLiveObjectService s = redisson.getLiveObjectService();

		TestREntityWithRMap t1 = new TestREntityWithRMap("111", null);
		t1 = s.persist(t1);

		TestREntityValueNested t2 = new TestREntityValueNested("122");
		t2 = s.persist(t2);

		RMap<String, String> map = redisson.<String, String>getMap("32123");
		t2.setValue(t1);
		t2.getValue().setValue(map);
		map.put("field", "123");
		assertEquals("123",
				s.get(TestREntityWithRMap.class, "111")
						.getValue().get("field"));
		assertEquals("123",
				s.get(TestREntityValueNested.class, "122")
						.getValue().getValue().get("field"));
	}

	@Test
	public void testSerializerable() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass t = new TestClass("55555");
		t = service.persist(t);
		assertTrue(Objects.equals("55555", t.getId()));

		t = new TestClass(90909l);
		t = service.persist(t);
		assertTrue(Objects.equals(90909l, t.getId()));

		t = new TestClass(90909);
		t = service.persist(t);
		assertTrue(Objects.equals(90909, t.getId()));

		t = new TestClass(new ObjectId(9090909));
		t = service.persist(t);
		assertTrue(Objects.equals(new ObjectId(9090909), t.getId()));

		t = new TestClass(new Byte("0"));
		t = service.persist(t);
		assertEquals(new Byte("0"), Byte.valueOf(t.getId().toString()));

		t = new TestClass((byte) 90);
		assertEquals((byte) 90, Byte.parseByte(t.getId().toString()));

		t = new TestClass((Serializable) Arrays.asList(1, 2, 3, 4));
		t = service.persist(t);
		List<Integer> l = new ArrayList();
		l.addAll(Arrays.asList(1, 2, 3, 4));
		assertTrue(l.removeAll((List) t.getId()));
		assertTrue(l.isEmpty());

		try {
			t = new TestClass(new int[]{1, 2, 3, 4, 5});
			t = service.persist(t);
			fail("Should not be here");
		} catch (Exception e) {
			assertEquals("RId value cannot be an array.", e.getMessage());
		}

		try {
			t = new TestClass(new byte[]{1, 2, 3, 4, 5});
			t = service.persist(t);
			fail("Should not be here");
		} catch (Exception e) {
			assertEquals("RId value cannot be an array.", e.getMessage());
		}
	}

	@Test
	public void testMergeList() {
		Customer customer = new Customer("12");
		Order order = new Order(customer);
		customer.getOrders().add(order);
		Order order2 = new Order(customer);
		customer.getOrders().add(order2);

        RLiveObjectService liveObjectService = redisson.getLiveObjectService();
        liveObjectService.merge(customer);

		Customer mergedCustomer = liveObjectService.get(Customer.class, "12");
		String mergedCustomerId = mergedCustomer.getId();
		assertThat(mergedCustomerId).isEqualTo(customer.getId());
        List<Order> mergedOrders = mergedCustomer.getOrders();
        int size = mergedOrders.size();
        assertThat(size).isEqualTo(2);
		for (Order orderElement : mergedOrders) {
            Long orderId = orderElement.getId();
            Customer fetchedCustomer = orderElement.getCustomer();
            String customerId = fetchedCustomer.getId();
            assertThat(orderId).isNotNull();
            assertThat(customerId).isEqualTo("12");
		}

		try {
			liveObjectService.persist(customer);
			fail("Should not be here");
		} catch (Exception e) {
			assertEquals("This REntity already exists.", e.getMessage());
		}
	}

	@Test
	public void testPersistList() {
		Customer customer = new Customer("12");
		Order order = new Order(customer);
		customer.getOrders().add(order);
		Order order2 = new Order(customer);
		customer.getOrders().add(order2);

		redisson.getLiveObjectService().persist(customer);

		customer = redisson.getLiveObjectService().get(Customer.class, "12");
		assertThat(customer.getOrders().size()).isEqualTo(2);
		for (Order orderElement : customer.getOrders()) {
			assertThat(orderElement.getId()).isNotNull();
			assertThat(orderElement.getCustomer().getId()).isEqualTo("12");
		}
	}

	@Test
	public void testPersist() {
		RLiveObjectService service = redisson.getLiveObjectService();

		TestClass ts = new TestClass(new ObjectId(100));
		ts.setValue("VALUE");
		ts.setContent(new TestREntity("123", null));
		ts.addEntry("1", "2");
		TestClass persisted = service.persist(ts);

		assertEquals(3, redisson.getKeys().count());
		assertEquals(1, persisted.getValues().size());
		assertEquals("123", ((TestREntity) persisted.getContent()).getName());
		assertEquals(new ObjectId(100), persisted.getId());
		assertEquals("VALUE", persisted.getValue());
		try {
			service.persist(ts);
			fail("Should not be here");
		} catch (Exception e) {
			assertEquals("This REntity already exists.", e.getMessage());
		}
	}

	@Test
	public void testMerge() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass ts = new TestClass(new ObjectId(100));
		ts.setValue("VALUE");
		TestClass merged = service.merge(ts);
		assertEquals(new ObjectId(100), merged.getId());
		assertEquals("VALUE", merged.getValue());
		try {
			service.persist(ts);
			fail("Should not be here");
		} catch (Exception e) {
			assertEquals("This REntity already exists.", e.getMessage());
		}
		ts = new TestClass(new ObjectId(100));
		ts.setCode("CODE");
		merged = service.merge(ts);
		assertNull(ts.getValue());
		assertEquals("VALUE", merged.getValue());
		assertEquals("CODE", merged.getCode());
	}

	@Test
	public void testDetach() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass ts = new TestClass(new ObjectId(100));
		ts.setValue("VALUE");
		ts.setCode("CODE");
		TestClass merged = service.merge(ts);
		assertEquals("VALUE", merged.getValue());
		assertEquals("CODE", merged.getCode());
		TestClass detach = service.detach(merged);
		assertEquals(ts, detach);
	}

	@Test
	public void testIsPhantom() {
		RLiveObjectService service = redisson.getLiveObjectService();
		assertFalse(service.isExists(new Object()));
		TestClass ts = new TestClass(new ObjectId(100));
		assertFalse(service.isExists(service.get(TestClass.class, new ObjectId(100))));
		assertFalse(service.isExists(ts));
		ts.setValue("VALUE");
		ts.setCode("CODE");
		TestClass persisted = service.persist(ts);
		assertTrue(service.isExists(service.get(TestClass.class, new ObjectId(100))));
		assertFalse(service.isExists(ts));
		assertTrue(service.isExists(persisted));
	}

	@Test
	public void testIsLiveObject() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass ts = new TestClass(new ObjectId(100));
		assertFalse(service.isLiveObject(ts));
		TestClass persisted = service.persist(ts);
		assertFalse(service.isLiveObject(ts));
		assertTrue(service.isLiveObject(persisted));
	}

	@Test
	public void testAsLiveObject() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass instance = new TestClass(new ObjectId(100));
		instance = service.persist(instance);

		RLiveObject liveObject = service.asLiveObject(instance);
		assertEquals(new ObjectId(100), liveObject.getLiveObjectId());
		try {
			service.asLiveObject(new Object());
			fail("Should not be here");
		} catch (Exception e) {
			assertTrue(e instanceof ClassCastException);
		}
	}

	@Test
	public void testClassRegistration() {
		RLiveObjectService service = redisson.getLiveObjectService();
		service.registerClass(TestClass.class);
		assertTrue(service.isClassRegistered(TestClass.class));
		RLiveObjectService newService = redisson.getLiveObjectService();
		assertTrue(newService.isClassRegistered(TestClass.class));
		RedissonClient newRedisson = Redisson.create(redisson.getConfig());
		assertFalse(newRedisson.getLiveObjectService().isClassRegistered(TestClass.class));
		newRedisson.shutdown(1, 5, TimeUnit.SECONDS);
	}

	@Test
	public void testClassUnRegistration() {
		RLiveObjectService service = redisson.getLiveObjectService();
		service.registerClass(TestClass.class);
		assertTrue(service.isClassRegistered(TestClass.class));
		RLiveObjectService newService = redisson.getLiveObjectService();
		RedissonClient newRedisson = Redisson.create(redisson.getConfig());
		newRedisson.getLiveObjectService().registerClass(TestClass.class);
		newService.unregisterClass(TestClass.class);
		assertFalse(service.isClassRegistered(TestClass.class));
		assertFalse(newService.isClassRegistered(TestClass.class));
		assertTrue(newRedisson.getLiveObjectService().isClassRegistered(TestClass.class));
		assertFalse(service.isClassRegistered(TestClass.class));
		assertFalse(newService.isClassRegistered(TestClass.class));
		newRedisson.shutdown(1, 5, TimeUnit.SECONDS);
	}

	@Test
	public void testGet() {
		RLiveObjectService service = redisson.getLiveObjectService();
		assertNull(service.get(TestClass.class, new ObjectId(100)));
		TestClass ts = new TestClass(new ObjectId(100));
		TestClass persisted = service.persist(ts);
		assertNotNull(service.get(TestClass.class, new ObjectId(100)));
		persisted.setCode("CODE");
		assertNotNull(service.get(TestClass.class, new ObjectId(100)));
	}

	@Test
	public void testRemoveByInstance() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass ts = new TestClass(new ObjectId(100));
		ts.setCode("CODE");
		TestClass persisted = service.persist(ts);
		assertTrue(service.isExists(persisted));
		service.delete(persisted);
		assertFalse(service.isExists(persisted));
	}

	@Test
	public void testRemoveById() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass ts = new TestClass(new ObjectId(100));
		ts.setCode("CODE");
		TestClass persisted = service.persist(ts);
		assertTrue(service.isExists(persisted));
		assertThat(service.delete(TestClass.class, new ObjectId(100))).isEqualTo(1);
		assertFalse(service.isExists(persisted));
	}

	@Test
	public void testCreate() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass ts = new TestClass();
		ts = service.persist(ts);
		UUID uuid = UUID.fromString(ts.getId().toString());
		assertEquals(4, uuid.version());

		TestClassID1 tc1 = new TestClassID1();
		tc1 = service.persist(tc1);
		assertEquals(new Long(1), tc1.getName());
		TestClassID2 tc2 = new TestClassID2();
		tc2 = service.persist(tc2);
		assertEquals(new Long(1), tc2.getName());
	}

	@Test
	public void testFindIds() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed1 t1 = new TestIndexed1();
		t1.setId("1");
		t1.setKeywords(Collections.singletonList("132323"));
		TestIndexed1 t2 = new TestIndexed1();
		t2.setId("2");
		t2.setKeywords(Collections.singletonList("fjdklj"));
		s.persist(t1, t2);

		Iterable<String> ids = s.findIds(TestIndexed1.class);
		assertThat(ids).containsExactlyInAnyOrder("1", "2");
	}

	@Test
	public void testMergeList2() {
		RLiveObjectService s = redisson.getLiveObjectService();
		TestIndexed1 t1 = new TestIndexed1();
		t1.setId("1");
		List<String> ws = new ArrayList<>();
		ws.add("word1");
		ws.add("word2");
		t1.setKeywords(ws);
		s.persist(t1);

		List<String> ws2 = new ArrayList<>();
		ws2.add("word3");
		t1.setKeywords(ws2);
		s.merge(t1);
		assertThat(t1.getKeywords()).containsExactly("word3");

		t1 = s.get(TestIndexed1.class, "1");
		assertThat(t1.getKeywords()).containsExactly("word3");
	}

	@Test
	public void testTransformation() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass ts = new TestClass();
		ts = service.persist(ts);

		HashMap<String, String> m = new HashMap<>();
		ts.setContent(m);
		assertFalse(HashMap.class.isAssignableFrom(ts.getContent().getClass()));
		assertTrue(RMap.class.isAssignableFrom(ts.getContent().getClass()));

		HashSet<String> s = new HashSet<>();
		ts.setContent(s);
		assertFalse(HashSet.class.isAssignableFrom(ts.getContent().getClass()));
		assertTrue(RSet.class.isAssignableFrom(ts.getContent().getClass()));

		TreeSet<String> ss = new TreeSet<>();
		ts.setContent(ss);
		assertFalse(TreeSet.class.isAssignableFrom(ts.getContent().getClass()));
		assertTrue(RSortedSet.class.isAssignableFrom(ts.getContent().getClass()));

		ArrayList<String> al = new ArrayList<>();
		ts.setContent(al);
		assertFalse(ArrayList.class.isAssignableFrom(ts.getContent().getClass()));
		assertTrue(RList.class.isAssignableFrom(ts.getContent().getClass()));

		ConcurrentHashMap<String, String> chm = new ConcurrentHashMap<>();
		ts.setContent(chm);
		assertFalse(ConcurrentHashMap.class.isAssignableFrom(ts.getContent().getClass()));
		assertTrue(RMap.class.isAssignableFrom(ts.getContent().getClass()));

		ArrayBlockingQueue<String> abq = new ArrayBlockingQueue<>(10);
		ts.setContent(abq);
		assertFalse(ArrayBlockingQueue.class.isAssignableFrom(ts.getContent().getClass()));
		assertTrue(RBlockingQueue.class.isAssignableFrom(ts.getContent().getClass()));

		ConcurrentLinkedQueue<String> clq = new ConcurrentLinkedQueue<>();
		ts.setContent(clq);
		assertFalse(ConcurrentLinkedQueue.class.isAssignableFrom(ts.getContent().getClass()));
		assertTrue(RQueue.class.isAssignableFrom(ts.getContent().getClass()));

		LinkedBlockingDeque<String> lbdq = new LinkedBlockingDeque<>();
		ts.setContent(lbdq);
		assertFalse(LinkedBlockingDeque.class.isAssignableFrom(ts.getContent().getClass()));
		assertTrue(RBlockingDeque.class.isAssignableFrom(ts.getContent().getClass()));

		LinkedList<String> ll = new LinkedList<>();
		ts.setContent(ll);
		assertFalse(LinkedList.class.isAssignableFrom(ts.getContent().getClass()));
		assertTrue(RDeque.class.isAssignableFrom(ts.getContent().getClass()));

	}

	@Test
	public void testNoTransformation() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClassNoTransformation ts = new TestClassNoTransformation();
		ts = service.persist(ts);

		HashMap<String, String> m = new HashMap<>();
		ts.setContent(m);
		assertTrue(HashMap.class.isAssignableFrom(ts.getContent().getClass()));
		assertFalse(RMap.class.isAssignableFrom(ts.getContent().getClass()));

		HashSet<String> s = new HashSet<>();
		ts.setContent(s);
		assertTrue(HashSet.class.isAssignableFrom(ts.getContent().getClass()));
		assertFalse(RSet.class.isAssignableFrom(ts.getContent().getClass()));

		TreeSet<String> ss = new TreeSet<>();
		ts.setContent(ss);
		assertTrue(TreeSet.class.isAssignableFrom(ts.getContent().getClass()));
		assertFalse(RSortedSet.class.isAssignableFrom(ts.getContent().getClass()));

		ArrayList<String> al = new ArrayList<>();
		ts.setContent(al);
		assertTrue(ArrayList.class.isAssignableFrom(ts.getContent().getClass()));
		assertFalse(RList.class.isAssignableFrom(ts.getContent().getClass()));

		ConcurrentHashMap<String, String> chm = new ConcurrentHashMap<>();
		ts.setContent(chm);
		assertTrue(ConcurrentHashMap.class.isAssignableFrom(ts.getContent().getClass()));
		assertFalse(RMap.class.isAssignableFrom(ts.getContent().getClass()));

		ConcurrentLinkedQueue<String> clq = new ConcurrentLinkedQueue<>();
		ts.setContent(clq);
		assertTrue(ConcurrentLinkedQueue.class.isAssignableFrom(ts.getContent().getClass()));
		assertFalse(RQueue.class.isAssignableFrom(ts.getContent().getClass()));

		LinkedBlockingDeque<String> lbdq = new LinkedBlockingDeque<>();
		ts.setContent(lbdq);
		assertTrue(LinkedBlockingDeque.class.isAssignableFrom(ts.getContent().getClass()));
		assertFalse(RBlockingDeque.class.isAssignableFrom(ts.getContent().getClass()));

		LinkedList<String> ll = new LinkedList<>();
		ts.setContent(ll);
		assertTrue(LinkedList.class.isAssignableFrom(ts.getContent().getClass()));
		assertFalse(RDeque.class.isAssignableFrom(ts.getContent().getClass()));

	}

	@Test
	public void test() {
		RLiveObjectService service = redisson.getLiveObjectService();

		MyObject object = new MyObject(20L);
		try {
			service.attach(object);
		} catch (Exception e) {
			assertEquals("Non-null value is required for the field with RId annotation.", e.getMessage());
		}
	}

	@Test
	public void testExpirable() throws InterruptedException {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass myObject = new TestClass();
		myObject = service.persist(myObject);
		myObject.setValue("123345");
		assertTrue(service.asLiveObject(myObject).isExists());
		service.asRMap(myObject).expire(1, TimeUnit.SECONDS);
		Thread.sleep(2000);
		assertFalse(service.asLiveObject(myObject).isExists());
	}

	@Test
	public void testMap() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass myObject = new TestClass();
		myObject = service.persist(myObject);

		myObject.setValue("123345");
		assertEquals("123345", service.asRMap(myObject).get("value"));
		service.asRMap(myObject).put("value", "9999");
		assertEquals("9999", myObject.getValue());
	}

	@Test
	public void testRObject() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass myObject = new TestClass();
		myObject = service.persist(myObject);
		try {
			((RObject) myObject).isExists();
		} catch (Exception e) {
			assertEquals("Please use RLiveObjectService instance for this type of functions", e.getMessage());
		}
	}

	@Test
	public void testStoreInnerObject() {
		RLiveObjectService service = redisson.getLiveObjectService();
		ObjectWithList so = new ObjectWithList();
		so = service.persist(so);

		SimpleObject s = new SimpleObject();
		s = service.persist(s);
		so.setSo(s);
		assertThat(s.getId()).isNotNull();
		so.getObjects().add(s);
		so = redisson.getLiveObjectService().detach(so);
		assertThat(so.getSo().getId()).isEqualTo(s.getId());
		assertThat(so.getObjects().get(0).getId()).isEqualTo(so.getSo().getId());
	}

	@Test
	public void testFieldWithoutIdSetter() {
		RLiveObjectService service = redisson.getLiveObjectService();
		SimpleObject so = new SimpleObject();
		so = service.persist(so);
		so.setValue(10L);

		so = redisson.getLiveObjectService().detach(so);
		assertThat(so.getId()).isNotNull();
		assertThat(so.getValue()).isEqualTo(10L);

		so = redisson.getLiveObjectService().get(SimpleObject.class, so.getId());
		assertThat(so.getId()).isNotNull();
		assertThat(so.getValue()).isEqualTo(10L);
	}

	@Test
	public void testCreateObjectsInRuntime() {
		RLiveObjectService service = redisson.getLiveObjectService();

		TestREntityWithMap so = new TestREntityWithMap();
		so = service.persist(so);

		Map<String, String> map1 = (Map<String, String>) so.getValue();
		map1.put("1", "2");

		so = redisson.getLiveObjectService().detach(so);
		Map<String, String> map2 = (Map<String, String>) so.getValue();
		assertThat(so.getName()).isNotNull();
		assertThat(map2).containsKey("1");
		assertThat(map2).containsValue("2");

		so = redisson.getLiveObjectService().get(TestREntityWithMap.class, so.getName());
		Map<String, String> map3 = (Map<String, String>) so.getValue();
		assertThat(so.getName()).isNotNull();
		assertThat(map3).containsKey("1");
		assertThat(map3).containsValue("2");
	}

	@Test
	public void testFieldAccessor() {
		RLiveObjectService service = redisson.getLiveObjectService();
		TestClass myObject = new TestClass();
		myObject = service.persist(myObject);

		myObject.setValue("123345");
		assertEquals("123345", myObject.get("value"));
		myObject.set("value", "9999");
		assertEquals("9999", myObject.get("value"));
		assertEquals("9999", myObject.getValue());
		try {
			myObject.get("555555");
		} catch (Exception e) {
			assertTrue(e instanceof NoSuchFieldException);
		}
		try {
			myObject.set("555555", "999");
		} catch (Exception e) {
			assertTrue(e instanceof NoSuchFieldException);
		}
	}

	@Test
	public void testCollectionRewrite() {
		Customer c = new Customer("123");
		c = redisson.getLiveObjectService().merge(c);

		Order o1 = new Order(c);
		o1 = redisson.getLiveObjectService().merge(o1);
		assertThat(o1.getId()).isEqualTo(1);
		c.getOrders().add(o1);

		Order o2 = new Order(c);
		o2 = redisson.getLiveObjectService().merge(o2);
		assertThat(o2.getId()).isEqualTo(2);
		c.getOrders().add(o2);

		assertThat(c.getOrders().size()).isEqualTo(2);

		assertThat(redisson.getKeys().count()).isEqualTo(7);

		List<Order> list = new ArrayList<>();
		Order o3 = new Order(c);
		o3 = redisson.getLiveObjectService().merge(o3);
		assertThat(o3.getId()).isEqualTo(3);
		list.add(o3);
		c.setOrders(list);

		assertThat(c.getOrders().size()).isEqualTo(1);
	}

	@Test
    // When a setter is encapsulated, kotlin optimizes by removing the setter inside the method.
    // There is no setter method accessible from inside the methods of a class.
	public void testSetterEncapsulation() {
		SetterEncapsulation se = new SetterEncapsulation();
        RLiveObjectService liveObjectService = redisson.getLiveObjectService();
        se = liveObjectService.persist(se);

        long count = redisson.getKeys().count();
        assertThat(count).isEqualTo(2);

		se.addItem("1", 1);
		se.addItem("2", 2);

        long count1 = redisson.getKeys().count();
        assertThat(count1).isEqualTo(3);

		se = liveObjectService.get(SetterEncapsulation.class, se.getId());

		assertThat(se.getItem("1")).isEqualTo(1);
		assertThat(se.getItem("2")).isEqualTo(2);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testObjectShouldNotBeAttached() {
		Customer customer = new Customer("12");
		customer = redisson.getLiveObjectService().persist(customer);
		Order order = new Order();
		customer.getOrders().add(order);
	}

	@Test
	public void testObjectShouldNotBeAttached2() {
		Customer customer = new Customer("12");
		Order order = new Order(customer);
		order = redisson.getLiveObjectService().persist(order);
	}

	@Test
	public void testDeleteList() {
		Customer customer = new Customer("12");
		Order order = new Order(customer);
		customer.getOrders().add(order);
		Order order2 = new Order(customer);
		customer.getOrders().add(order2);

		order = redisson.getLiveObjectService().persist(order);
		assertThat(redisson.getKeys().count()).isEqualTo(5);

		redisson.getLiveObjectService().delete(order.getCustomer());
		assertThat(redisson.getKeys().count()).isEqualTo(1);
	}

	@Test
	public void testDeleteNotExisted() {
		RLiveObjectService service = redisson.getLiveObjectService();
		assertThat(service.delete(Customer.class, "id")).isZero();
	}

	@Test
	public void testDeleteMultipleIds() {
		Customer customer1 = new Customer("1");
		Customer customer2 = new Customer("2");
		RLiveObjectService ls = redisson.getLiveObjectService();
		ls.persist(customer1, customer2);
		assertThat(ls.delete(Customer.class, "1", "2")).isEqualTo(2);
		assertThat(redisson.getKeys().count()).isZero();
	}

	@Test
	public void testDelete() {
		Customer customer = new Customer("12");
		Order order = new Order(customer);
		order = redisson.getLiveObjectService().persist(order);
		assertThat(redisson.getKeys().count()).isEqualTo(3);

		Customer persistedCustomer = order.getCustomer();
		redisson.getLiveObjectService().delete(order);
		assertThat(redisson.getKeys().count()).isEqualTo(2);

		redisson.getLiveObjectService().delete(persistedCustomer);
		assertThat(redisson.getKeys().count()).isEqualTo(1);
	}

	@Test
	public void testObjectShouldBeAttached() {
		Customer customer = new Customer("12");
		customer = redisson.getLiveObjectService().persist(customer);
		Order order = new Order();
		order = redisson.getLiveObjectService().persist(order);
		customer.getOrders().add(order);

		customer = redisson.getLiveObjectService().detach(customer);
		assertThat(customer.getClass()).isSameAs(Customer.class);
		assertThat(customer.getId()).isNotNull();
		List<Order> orders = customer.getOrders();
		assertThat(orders.get(0)).isNotNull();

		customer = redisson.getLiveObjectService().get(Customer.class, customer.getId());
		assertThat(customer.getId()).isNotNull();
		assertThat(customer.getOrders().get(0)).isNotNull();
	}

	@Test
	public void testCyclicRefsDuringDetach() {
		Customer customer = new Customer("12");
		customer = redisson.getLiveObjectService().persist(customer);
		Order order = new Order();
		order = redisson.getLiveObjectService().persist(order);
		order.setCustomer(customer);
		customer.getOrders().add(order);

		customer = redisson.getLiveObjectService().detach(customer);

		assertThat(customer.getClass()).isSameAs(Customer.class);
		assertThat(customer.getId()).isNotNull();
		List<Order> orders = customer.getOrders();
		assertThat(orders.get(0).getCustomer()).isSameAs(customer);

		customer = redisson.getLiveObjectService().get(Customer.class, customer.getId());

		assertThat(customer.getId()).isNotNull();
		Order o = customer.getOrders().get(0);
		assertThat(o.getCustomer().getId()).isEqualTo(customer.getId());
	}

	@Test
	public void testWithoutIdSetterGetter() {
		ClassWithoutIdSetterGetter sg = new ClassWithoutIdSetterGetter();
		sg = redisson.getLiveObjectService().persist(sg);
	}

	@Test
	public void testProtectedConstructor() {
		ClassWithoutIdSetterGetter sg = new ClassWithoutIdSetterGetter("1234");
		sg = redisson.getLiveObjectService().persist(sg);
		assertThat(sg.getName()).isEqualTo("1234");
	}

	@Test
	public void testEnum() {
		RLiveObjectService liveObjectService = redisson.getLiveObjectService();
		liveObjectService.registerClass(TestEnum.class);

		String id = "1";
		TestEnum entry = new TestEnum();
		entry.setId(id);
		entry.setMyEnum1(TestEnum.MyEnum.A);
		entry = liveObjectService.persist(entry);

		TestEnum liveEntry = liveObjectService.get(TestEnum.class, id);
		assertThat(liveEntry.getMyEnum1()).isEqualTo(TestEnum.MyEnum.A);
		assertThat(liveEntry.getMyEnum2()).isNull();

		entry.setMyEnum2(TestEnum.MyEnum.B);
		assertThat(liveEntry.getMyEnum2()).isEqualTo(TestEnum.MyEnum.B);
	}

	@Test
	public void testInheritedREntity() {
		Dog d = new Dog("Fido");
		d.setBreed("lab");

		d = redisson.getLiveObjectService().persist(d);

		assertThat(d.getName()).isEqualTo("Fido");
		assertThat(d.getBreed()).isEqualTo("lab");
	}

	@Test
	public void testMapOfInheritedEntity() {
		RMap<String, Dog> dogs = redisson.getMap("dogs");
		Dog d = new Dog("Fido");
		d = redisson.getLiveObjectService().persist(d);
		d.setBreed("lab");
		dogs.put("key", d);
		dogs = redisson.getMap("dogs");
		assertThat(dogs.size()).isEqualTo(1);
		assertThat(dogs.get("key").getBreed()).isEqualTo("lab");
	}

	@Test
	public void testCyclicRefsWithInheritedREntity() {
		MyCustomer customer = new MyCustomer("12");
		customer = redisson.getLiveObjectService().persist(customer);
		Order order = new Order();
		order = redisson.getLiveObjectService().persist(order);
		order.setCustomer(customer);
		customer.getOrders().add(order);
		Order special = new Order();
		special = redisson.getLiveObjectService().persist(special);
		order.setCustomer(customer);
		customer.addSpecialOrder(special);

		customer = redisson.getLiveObjectService().detach(customer);
		assertThat(customer.getClass()).isSameAs(MyCustomer.class);
		assertThat(customer.getId()).isNotNull();
		List<Order> orders = customer.getOrders();
		assertThat(orders.get(0)).isNotNull();
		List<Order> specials = customer.getSpecialOrders();
		assertThat(specials.get(0)).isNotNull();

		customer = redisson.getLiveObjectService().get(MyCustomer.class, customer.getId());
		assertThat(customer.getId()).isNotNull();
		assertThat(customer.getOrders().get(0)).isNotNull();
		assertThat(customer.getSpecialOrders().get(0)).isNotNull();
	}

	@Test
	public void testStoreInnerObjectWithInheritedREntity() {
		RLiveObjectService service = redisson.getLiveObjectService();
		MyObjectWithList so = new MyObjectWithList();
		so = service.persist(so);

		SimpleObject s = new SimpleObject();
		s = service.persist(s);

		so.setSo(s);
		assertThat(s.getId()).isNotNull();
		so.getObjects().add(s);
		so.setName("name");

		so = redisson.getLiveObjectService().detach(so);
		assertThat(so.getSo().getId()).isEqualTo(s.getId());
		assertThat(so.getObjects().get(0).getId()).isEqualTo(so.getSo().getId());
		assertThat(so.getName()).isEqualTo("name");
	}

	@Test(timeout = 40 * 1000)
	public void testBatchedPersist() {
		RLiveObjectService s = redisson.getLiveObjectService();

		List<TestREntity> objects = new ArrayList<>();
		int objectsAmount = 1000000;
		for (int i = 0; i < objectsAmount; i++) {
			TestREntity e = new TestREntity();
			e.setName("" + i);
			e.setValue("value" + i);
			objects.add(e);
		}
		List<Object> attachedObjects = s.persist(objects.toArray());
		assertThat(attachedObjects).hasSize(objectsAmount);

		assertThat(redisson.getKeys().count()).isEqualTo(objectsAmount);
	}

	@Test
	public void testIsAccessor() {
		HasIsAccessor o = new HasIsAccessor();
		o.setGood(true);
		o = redisson.getLiveObjectService().persist(o);
		assertThat(o.getGood()).isEqualTo(true);
	}

}
