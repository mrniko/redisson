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

import java.net.InetSocketAddress;

/**
 * Redis node interface
 *
 * @author Nikita Koksharov
 *
 */
public interface Node {

  /**
   * Get Redis node address
   *
   * @return
   */
  InetSocketAddress getAddr();

  /**
   * Ping Redis node by PING command.
   *
   * @return <code>true</code> if PONG received, <code>false</code> otherwise
   */
  boolean ping();

}
