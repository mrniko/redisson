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
package org.redisson.client.protocol;

import java.util.ArrayList;
import java.util.List;

import io.netty.util.concurrent.Promise;

public class CommandsData implements QueueCommand {

  private final List<CommandData<?, ?>> commands;
  private final Promise<Void> promise;

  public CommandsData(Promise<Void> promise, List<CommandData<?, ?>> commands) {
    super();
    this.promise = promise;
    this.commands = commands;
  }

  public Promise<Void> getPromise() {
    return promise;
  }

  public List<CommandData<?, ?>> getCommands() {
    return commands;
  }

  @Override
  public List<CommandData<Object, Object>> getPubSubOperations() {
    List<CommandData<Object, Object>> result = new ArrayList<CommandData<Object, Object>>();
    for (CommandData<?, ?> commandData : commands) {
      if (PUBSUB_COMMANDS.equals(commandData.getCommand().getName())) {
        result.add((CommandData<Object, Object>) commandData);
      }
    }
    return result;
  }

}
