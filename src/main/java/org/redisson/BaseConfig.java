/**
 * Copyright 2014 Nikita Koksharov, Nickolay Borbit
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
package org.redisson;


class BaseConfig<T extends BaseConfig<T>> {

    /**
     * Redis operation execution timeout.
     * Then amount is reached exception will be thrown in case of <b>sync</b> operation usage
     * or <code>Future</code> callback fails in case of <b>async</b> operation.
     */
    private int timeout = 60000;

    private int retryAttempts = 5;

    private int retryInterval = 1000;

    /**
     * Database index used for Redis connection
     */
    private int database = 0;

    /**
     * Password for Redis authentication. Should be null if not needed
     */
    private String password;

    /**
     * Subscriptions per Redis connection limit
     */
    private int subscriptionsPerConnection = 5;

    BaseConfig() {
    }

    BaseConfig(T config) {
        setPassword(config.getPassword());
        setSubscriptionsPerConnection(config.getSubscriptionsPerConnection());
        setRetryAttempts(config.getRetryAttempts());
        setRetryInterval(config.getRetryInterval());
        setDatabase(config.getDatabase());
        setTimeout(config.getTimeout());
    }

    /**
     * Subscriptions per Redis connection limit
     * Default is 5
     *
     * @param subscriptionsPerConnection
     */
    public T setSubscriptionsPerConnection(int subscriptionsPerConnection) {
        this.subscriptionsPerConnection = subscriptionsPerConnection;
        return (T) this;
    }
    public int getSubscriptionsPerConnection() {
        return subscriptionsPerConnection;
    }

    /**
     * Password for Redis authentication. Should be null if not needed
     * Default is <code>null</code>
     *
     * @param password
     */
    public T setPassword(String password) {
        this.password = password;
        return (T) this;
    }
    public String getPassword() {
        return password;
    }

    /**
     * Reconnection attempts amount.
     * Then amount is reached exception will be thrown in case of <b>sync</b> operation usage
     * or <code>Future</code> callback fails in case of <b>async</b> operation.
     *
     * Used then connection with redis server is down.
     *
     * @param retryAttempts
     */
    public T setRetryAttempts(int retryAttempts) {
        this.retryAttempts = retryAttempts;
        return (T) this;
    }
    public int getRetryAttempts() {
        return retryAttempts;
    }

    /**
     * Time pause before next reconnection attempt.
     *
     * Used then connection with redis server is down.
     *
     * @param retryInterval - time in milliseconds
     */
    public T setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
        return (T) this;
    }
    public int getRetryInterval() {
        return retryInterval;
    }

    /**
     * Database index used for Redis connection
     * Default is <code>0</code>
     *
     * @param database
     */
    public T setDatabase(int database) {
        this.database = database;
        return (T) this;
    }
    public int getDatabase() {
        return database;
    }

    /**
     * Redis operation execution timeout.
     * Then amount is reached exception will be thrown in case of <b>sync</b> operation usage
     * or <code>Future</code> callback fails in case of <b>async</b> operation.
     *
     * @param timeout in milliseconds
     */
    public T setTimeout(int timeout) {
        this.timeout = timeout;
        return (T) this;
    }
    public int getTimeout() {
        return timeout;
    }

}
