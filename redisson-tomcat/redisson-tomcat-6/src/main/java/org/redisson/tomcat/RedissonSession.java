/**
 * Copyright 2018 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.tomcat;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.catalina.session.StandardSession;
import org.redisson.api.RMap;
import org.redisson.api.RTopic;
import org.redisson.tomcat.RedissonSessionManager.ReadMode;
import org.redisson.tomcat.RedissonSessionManager.UpdateMode;

/**
 * Redisson Session object for Apache Tomcat
 * 
 * @author Nikita Koksharov
 *
 */
public class RedissonSession extends StandardSession {

    private final RedissonSessionManager redissonManager;
    private final Map<String, Object> attrs;
    private RMap<String, Object> map;
    private RTopic<AttributeMessage> topic;
    private final RedissonSessionManager.ReadMode readMode;
    private final UpdateMode updateMode;
    
    public RedissonSession(RedissonSessionManager manager, ReadMode readMode, UpdateMode updateMode) {
        super(manager);
        this.redissonManager = manager;
        this.readMode = readMode;
        this.updateMode = updateMode;
        
        try {
            Field attr = StandardSession.class.getDeclaredField("attributes");
            attrs = (Map<String, Object>) attr.get(this);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static final long serialVersionUID = -2518607181636076487L;

    @Override
    public Object getAttribute(String name) {
        if (readMode == ReadMode.REDIS) {
            return map.get(name);
        }

        return super.getAttribute(name);
    }
    
    @Override
    public void setId(String id, boolean notify) {
        super.setId(id, notify);
        map = redissonManager.getMap(id);
        topic = redissonManager.getTopic();
    }
    
    public void delete() {
        map.delete();
        if (readMode == ReadMode.MEMORY) {
            topic.publish(new AttributesClearMessage(getId()));
        }
        map = null;
    }
    
    @Override
    public void setCreationTime(long time) {
        super.setCreationTime(time);

        if (map != null) {
            Map<String, Object> newMap = new HashMap<String, Object>(3);
            newMap.put("session:creationTime", creationTime);
            newMap.put("session:lastAccessedTime", lastAccessedTime);
            newMap.put("session:thisAccessedTime", thisAccessedTime);
            map.putAll(newMap);
            if (readMode == ReadMode.MEMORY) {
                topic.publish(new AttributesPutAllMessage(getId(), newMap));
            }
        }
    }
    
    @Override
    public void access() {
        super.access();
        
        if (map != null) {
            Map<String, Object> newMap = new HashMap<String, Object>(2);
            newMap.put("session:lastAccessedTime", lastAccessedTime);
            newMap.put("session:thisAccessedTime", thisAccessedTime);
            map.putAll(newMap);
            if (readMode == ReadMode.MEMORY) {
                topic.publish(new AttributesPutAllMessage(getId(), newMap));
            }
            if (getMaxInactiveInterval() >= 0) {
                map.expire(getMaxInactiveInterval(), TimeUnit.SECONDS);
            }
        }
    }
    
    @Override
    public void setMaxInactiveInterval(int interval) {
        super.setMaxInactiveInterval(interval);
        
        if (map != null) {
            fastPut("session:maxInactiveInterval", maxInactiveInterval);
            if (maxInactiveInterval >= 0) {
                map.expire(getMaxInactiveInterval(), TimeUnit.SECONDS);
            }
        }
    }
    
    private void fastPut(String name, Object value) {
        map.fastPut(name, value);
        if (readMode == ReadMode.MEMORY) {
            topic.publish(new AttributeUpdateMessage(getId(), name, value));
        }
    }
    
    @Override
    public void setValid(boolean isValid) {
        super.setValid(isValid);
        
        if (map != null) {
            if (!isValid && !map.isExists()) {
                return;
            }

            fastPut("session:isValid", isValid);
        }
    }
    
    @Override
    public void setNew(boolean isNew) {
        super.setNew(isNew);
        
        if (map != null) {
            fastPut("session:isNew", isNew);
        }
    }
    
    @Override
    public void endAccess() {
        boolean oldValue = isNew;
        super.endAccess();

        if (isNew != oldValue) {
            fastPut("session:isNew", isNew);
        }
    }
    
    public void superSetAttribute(String name, Object value, boolean notify) {
        super.setAttribute(name, value, notify);
    }
    
    @Override
    public void setAttribute(String name, Object value, boolean notify) {
        super.setAttribute(name, value, notify);
        
        if (updateMode == UpdateMode.DEFAULT && map != null && value != null) {
            fastPut(name, value);
        }
    }
    
    public void superRemoveAttributeInternal(String name, boolean notify) {
        super.removeAttributeInternal(name, notify);
    }
    
    @Override
    protected void removeAttributeInternal(String name, boolean notify) {
        super.removeAttributeInternal(name, notify);
        
        if (updateMode == UpdateMode.DEFAULT && map != null) {
            map.fastRemove(name);
            if (readMode == ReadMode.MEMORY) {
                topic.publish(new AttributeRemoveMessage(getId(), name));
            }
        }
    }
    
    public void save() {
        Map<String, Object> newMap = new HashMap<String, Object>();
        newMap.put("session:creationTime", creationTime);
        newMap.put("session:lastAccessedTime", lastAccessedTime);
        newMap.put("session:thisAccessedTime", thisAccessedTime);
        newMap.put("session:maxInactiveInterval", maxInactiveInterval);
        newMap.put("session:isValid", isValid);
        newMap.put("session:isNew", isNew);
        
        if (attrs != null) {
            for (Entry<String, Object> entry : attrs.entrySet()) {
                newMap.put(entry.getKey(), entry.getValue());
            }
        }
        
        map.putAll(newMap);
        if (readMode == ReadMode.MEMORY) {
            topic.publish(new AttributesPutAllMessage(getId(), newMap));
        }
        
        if (maxInactiveInterval >= 0) {
            map.expire(getMaxInactiveInterval(), TimeUnit.SECONDS);
        }
    }
    
    public void load(Map<String, Object> attrs) {
        Long creationTime = (Long) attrs.remove("session:creationTime");
        if (creationTime != null) {
            this.creationTime = creationTime;
        }
        Long lastAccessedTime = (Long) attrs.remove("session:lastAccessedTime");
        if (lastAccessedTime != null) {
            this.lastAccessedTime = lastAccessedTime;
        }
        Integer maxInactiveInterval = (Integer) attrs.remove("session:maxInactiveInterval");
        if (maxInactiveInterval != null) {
            this.maxInactiveInterval = maxInactiveInterval;
        }
        Long thisAccessedTime = (Long) attrs.remove("session:thisAccessedTime");
        if (thisAccessedTime != null) {
            this.thisAccessedTime = thisAccessedTime;
        }
        Boolean isValid = (Boolean) attrs.remove("session:isValid");
        if (isValid != null) {
            this.isValid = isValid;
        }
        Boolean isNew = (Boolean) attrs.remove("session:isNew");
        if (isNew != null) {
            this.isNew = isNew;
        }

        for (Entry<String, Object> entry : attrs.entrySet()) {
            super.setAttribute(entry.getKey(), entry.getValue(), false);
        }
    }
    
}
