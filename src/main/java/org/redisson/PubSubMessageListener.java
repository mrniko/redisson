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
package org.redisson;

import org.redisson.client.RedisPubSubListener;
import org.redisson.client.protocol.pubsub.PubSubType;
import org.redisson.core.MessageListener;

/**
 *
 * @author Nikita Koksharov
 *
 * @param <K>
 * @param <V>
 */
public class PubSubMessageListener<V> implements RedisPubSubListener<V> {

  private final MessageListener<V> listener;
  private final String name;

  public String getName() {
    return name;
  }

  public PubSubMessageListener(MessageListener<V> listener, String name) {
    super();
    this.listener = listener;
    this.name = name;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((listener == null) ? 0 : listener.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PubSubMessageListener other = (PubSubMessageListener) obj;
    if (listener == null) {
      if (other.listener != null)
        return false;
    } else if (!listener.equals(other.listener))
      return false;
    return true;
  }

  @Override
  public void onMessage(String channel, V message) {
    // could be subscribed to multiple channels
    if (name.equals(channel)) {
      listener.onMessage(channel, message);
    }
  }

  @Override
  public void onPatternMessage(String pattern, String channel, V message) {
    // could be subscribed to multiple channels
    if (name.equals(pattern)) {
      listener.onMessage(channel, message);
    }
  }

  @Override
  public boolean onStatus(PubSubType type, String channel) {
    return false;
  }

}
