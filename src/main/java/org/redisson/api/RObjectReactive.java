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
package org.redisson.api;

import org.reactivestreams.Publisher;

/**
 * Base interface for all Redisson objects
 *
 * @author Nikita Koksharov
 *
 */
public interface RObjectReactive {

  String getName();

  /**
   * Transfer a object from a source Redis instance to a destination Redis instance in mode
   *
   * @param host - destination host
   * @param port - destination port
   * @param database - destination database
   * @return
   */
  Publisher<Void> migrate(String host, int port, int database);

  /**
   * Move object to another database in mode
   *
   * @param database
   * @return <code>true</code> if key was moved <code>false</code> if not
   */
  Publisher<Boolean> move(int database);

  /**
   * Delete object in mode
   *
   * @return <code>true</code> if object was deleted <code>false</code> if not
   */
  Publisher<Boolean> delete();

  /**
   * Rename current object key to <code>newName</code> in mode
   *
   * @param newName
   * @return
   */
  Publisher<Void> rename(String newName);

  /**
   * Rename current object key to <code>newName</code> in mode only if new key is not exists
   *
   * @param newName
   * @return
   */
  Publisher<Boolean> renamenx(String newName);

  /**
   * Check object existence
   *
   * @return <code>true</code> if object exists and <code>false</code> otherwise
   */
  Publisher<Boolean> isExists();

}
