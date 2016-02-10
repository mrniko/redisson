/**
 * Copyright 2014 Nikita Koksharov, Nickolay Borbit
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.redisson.core;

/**
 * Bloom filter based on 64-bit hash derived from 128-bit hash (xxHash + FarmHash).
 *
 * Code parts from Guava BloomFilter
 *
 * @author Nikita Koksharov
 *
 * @param <T>
 */
public interface RBloomFilter<T> extends RExpirable {

  boolean add(T object);

  boolean contains(T object);

  /**
   * Initializes Bloom filter params (size and hashIterations) calculated from
   * <code>expectedInsertions</code> and <code>falseProbability</code> Stores config to Redis
   * server.
   *
   * @param expectedInsertions
   * @param falseProbability
   * @return <code>true</code> if Bloom filter initialized <code>false</code> if Bloom filter
   *         already has been initialized
   */
  boolean tryInit(long expectedInsertions, double falseProbability);

  long getExpectedInsertions();

  double getFalseProbability();

  long getSize();

  int getHashIterations();

  /**
   * Calculates probabilistic number of elements already added to Bloom filter.
   *
   * @return
   */
  int count();

}
