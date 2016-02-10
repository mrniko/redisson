package org.redisson;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;

import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.redisson.core.RDeque;

public class RedissonDequeTest extends BaseTest {

  @Test
  public void testRemoveLastOccurrence() {
    RDeque<Integer> queue1 = redisson.getDeque("deque1");
    queue1.addFirst(3);
    queue1.addFirst(1);
    queue1.addFirst(2);
    queue1.addFirst(3);

    queue1.removeLastOccurrence(3);

    MatcherAssert.assertThat(queue1, Matchers.containsInAnyOrder(3, 2, 1));
  }

  @Test
  public void testRemoveFirstOccurrence() {
    RDeque<Integer> queue1 = redisson.getDeque("deque1");
    queue1.addFirst(3);
    queue1.addFirst(1);
    queue1.addFirst(2);
    queue1.addFirst(3);

    queue1.removeFirstOccurrence(3);

    MatcherAssert.assertThat(queue1, Matchers.containsInAnyOrder(2, 1, 3));
  }

  @Test
  public void testRemoveLast() {
    RDeque<Integer> queue1 = redisson.getDeque("deque1");
    queue1.addFirst(1);
    queue1.addFirst(2);
    queue1.addFirst(3);

    Assert.assertEquals(1, (int) queue1.removeLast());
    Assert.assertEquals(2, (int) queue1.removeLast());
    Assert.assertEquals(3, (int) queue1.removeLast());
  }

  @Test
  public void testRemoveFirst() {
    RDeque<Integer> queue1 = redisson.getDeque("deque1");
    queue1.addFirst(1);
    queue1.addFirst(2);
    queue1.addFirst(3);

    Assert.assertEquals(3, (int) queue1.removeFirst());
    Assert.assertEquals(2, (int) queue1.removeFirst());
    Assert.assertEquals(1, (int) queue1.removeFirst());
  }

  @Test
  public void testPeek() {
    RDeque<Integer> queue1 = redisson.getDeque("deque1");
    Assert.assertNull(queue1.peekFirst());
    Assert.assertNull(queue1.peekLast());
    queue1.addFirst(2);
    Assert.assertEquals(2, (int) queue1.peekFirst());
    Assert.assertEquals(2, (int) queue1.peekLast());
  }

  @Test
  public void testPollLastAndOfferFirstTo() {
    RDeque<Integer> queue1 = redisson.getDeque("deque1");
    queue1.addFirst(3);
    queue1.addFirst(2);
    queue1.addFirst(1);

    RDeque<Integer> queue2 = redisson.getDeque("deque2");
    queue2.addFirst(6);
    queue2.addFirst(5);
    queue2.addFirst(4);

    queue1.pollLastAndOfferFirstTo(queue2);
    MatcherAssert.assertThat(queue2, Matchers.contains(3, 4, 5, 6));
  }

  @Test
  public void testAddFirstOrigin() {
    Deque<Integer> queue = new ArrayDeque<Integer>();
    queue.addFirst(1);
    queue.addFirst(2);
    queue.addFirst(3);

    MatcherAssert.assertThat(queue, Matchers.contains(3, 2, 1));
  }

  @Test
  public void testAddFirst() {
    RDeque<Integer> queue = redisson.getDeque("deque");
    queue.addFirst(1);
    queue.addFirst(2);
    queue.addFirst(3);

    MatcherAssert.assertThat(queue, Matchers.contains(3, 2, 1));
  }

  @Test
  public void testAddLastOrigin() {
    Deque<Integer> queue = new ArrayDeque<Integer>();
    queue.addLast(1);
    queue.addLast(2);
    queue.addLast(3);

    MatcherAssert.assertThat(queue, Matchers.contains(1, 2, 3));
  }

  @Test
  public void testAddLast() {
    RDeque<Integer> queue = redisson.getDeque("deque");
    queue.addLast(1);
    queue.addLast(2);
    queue.addLast(3);

    MatcherAssert.assertThat(queue, Matchers.contains(1, 2, 3));
  }

  @Test
  public void testOfferFirstOrigin() {
    Deque<Integer> queue = new ArrayDeque<Integer>();
    queue.offerFirst(1);
    queue.offerFirst(2);
    queue.offerFirst(3);

    MatcherAssert.assertThat(queue, Matchers.contains(3, 2, 1));
  }

  @Test
  public void testOfferFirst() {
    RDeque<Integer> queue = redisson.getDeque("deque");
    queue.offerFirst(1);
    queue.offerFirst(2);
    queue.offerFirst(3);

    MatcherAssert.assertThat(queue, Matchers.contains(3, 2, 1));
  }

  @Test
  public void testOfferLastOrigin() {
    Deque<Integer> queue = new ArrayDeque<Integer>();
    queue.offerLast(1);
    queue.offerLast(2);
    queue.offerLast(3);

    MatcherAssert.assertThat(queue, Matchers.contains(1, 2, 3));

    Assert.assertEquals((Integer) 1, queue.poll());
  }

  @Test
  public void testDescendingIteratorOrigin() {
    final Deque<Integer> queue = new ArrayDeque<Integer>();
    queue.addAll(Arrays.asList(1, 2, 3));

    MatcherAssert.assertThat(new Iterable<Integer>() {

      @Override
      public Iterator<Integer> iterator() {
        return queue.descendingIterator();
      }

    }, Matchers.contains(3, 2, 1));
  }

  @Test
  public void testDescendingIterator() {
    final RDeque<Integer> queue = redisson.getDeque("deque");
    queue.addAll(Arrays.asList(1, 2, 3));

    MatcherAssert.assertThat(new Iterable<Integer>() {

      @Override
      public Iterator<Integer> iterator() {
        return queue.descendingIterator();
      }

    }, Matchers.contains(3, 2, 1));
  }

}
