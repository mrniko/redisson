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
package org.redisson.connection.decoder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.redisson.client.handler.State;
import org.redisson.client.protocol.decoder.MultiDecoder;

import io.netty.buffer.ByteBuf;

public class MapGetAllDecoder implements MultiDecoder<Map<Object, Object>> {

  private final List<Object> args;

  public MapGetAllDecoder(List<Object> args) {
    this.args = args;
  }

  @Override
  public Object decode(ByteBuf buf, State state) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isApplicable(int paramNum, State state) {
    return false;
  }

  @Override
  public Map<Object, Object> decode(List<Object> parts, State state) {
    Map<Object, Object> result = new HashMap<Object, Object>(parts.size());
    for (int index = 0; index < args.size() - 1; index++) {
      Object value = parts.get(index);
      if (value == null) {
        continue;
      }
      result.put(args.get(index + 1), value);
    }
    return result;
  }

}
