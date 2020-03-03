/**
 * Copyright (c) 2013-2020 Nikita Koksharov
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
package org.redisson.config;

import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.redisson.client.RedisClientConfig.TlsVersion;

/**
 * 
 * @author Nikita Koksharov
 *
 * @param <T> config type
 */
class BaseConfig<T extends BaseConfig<T>> {
    
    private static final Logger log = LoggerFactory.getLogger("config");

    /**
     * If pooled connection not used for a <code>timeout</code> time
     * and current connections amount bigger than minimum idle connections pool size,
     * then it will closed and removed from pool.
     * Value in milliseconds.
     *
     */
    private int idleConnectionTimeout = 10000;

    /**
     * Ping timeout used in <code>Node.ping</code> and <code>Node.pingAll<code> operation.
     * Value in milliseconds.
     *
     */
    private int pingTimeout = 1000;

    /**
     * Timeout during connecting to any Redis server.
     * Value in milliseconds.
     *
     */
    private int connectTimeout = 10000;

    /**
     * Redis server response timeout. Starts to countdown when Redis command was succesfully sent.
     * Value in milliseconds.
     *
     */
    private int timeout = 3000;

    private int retryAttempts = 3;

    private int retryInterval = 1500;

    /**
     * Password for Redis authentication. Should be null if not needed
     */
    private String password;

    /**
     * TLS version for the client to use in the TLS handshake to instances.
     */
    private TlsVersion tlsVersion;

    /**
     * Subscriptions per Redis connection limit
     */
    private int subscriptionsPerConnection = 5;

    /**
     * Name of client connection
     */
    private String clientName;

    private boolean sslEnableEndpointIdentification = true;
    
    private SslProvider sslProvider = SslProvider.JDK;
    
    private URL sslTruststore;
    
    private String sslTruststorePassword;
    
    private URL sslKeystore;
    
    private String sslKeystorePassword;

    private int pingConnectionInterval;

    private boolean keepAlive;
    
    private boolean tcpNoDelay;

    
    BaseConfig() {
    }

    BaseConfig(T config) {
        setTlsVersion(config.getTlsVersion());
        setPassword(config.getPassword());
        setSubscriptionsPerConnection(config.getSubscriptionsPerConnection());
        setRetryAttempts(config.getRetryAttempts());
        setRetryInterval(config.getRetryInterval());
        setTimeout(config.getTimeout());
        setClientName(config.getClientName());
        setPingTimeout(config.getPingTimeout());
        setConnectTimeout(config.getConnectTimeout());
        setIdleConnectionTimeout(config.getIdleConnectionTimeout());
        setSslEnableEndpointIdentification(config.isSslEnableEndpointIdentification());
        setSslProvider(config.getSslProvider());
        setSslTruststore(config.getSslTruststore());
        setSslTruststorePassword(config.getSslTruststorePassword());
        setSslKeystore(config.getSslKeystore());
        setSslKeystorePassword(config.getSslKeystorePassword());
        setPingConnectionInterval(config.getPingConnectionInterval());
        setKeepAlive(config.isKeepAlive());
        setTcpNoDelay(config.isTcpNoDelay());
    }

    /**
     * Subscriptions per Redis connection limit
     * Default is 5
     *
     * @param subscriptionsPerConnection amount
     * @return config
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
     * @param password for connection
     * @return config
     */
    public T setPassword(String password) {
        this.password = password;
        return (T) this;
    }

    public String getPassword() {
        return password;
    }

    /**
     * Version for client to initiate the TLS handsake with.
     * Default is <code>null</code>
     *
     * @param tlsVersion options : TLSv1, TLSv1.1, TLSv1.2
     * @return config
     */
    public T setTlsVersion(TlsVersion tlsVersion) {
        this.tlsVersion = tlsVersion;
        return (T) this;
    }

    public TlsVersion getTlsVersion() {
        return tlsVersion;
    }


    /**
     * Error will be thrown if Redis command can't be sent to Redis server after <code>retryAttempts</code>.
     * But if it sent successfully then <code>timeout</code> will be started.
     * <p>
     * Default is <code>3</code> attempts
     *
     * @see #timeout
     * @param retryAttempts - retry attempts
     * @return config
     */
    public T setRetryAttempts(int retryAttempts) {
        this.retryAttempts = retryAttempts;
        return (T) this;
    }

    public int getRetryAttempts() {
        return retryAttempts;
    }

    /**
     * Defines time interval for another one attempt send Redis command 
     * if it hasn't been sent already.
     * 
     * <p>
     * Default is <code>1500</code> milliseconds
     *
     * @param retryInterval - time in milliseconds
     * @return config
     */
    public T setRetryInterval(int retryInterval) {
        this.retryInterval = retryInterval;
        return (T) this;
    }

    public int getRetryInterval() {
        return retryInterval;
    }

    /**
     * Redis server response timeout. Starts to countdown when Redis command has been successfully sent.
     * <p>
     * Default is <code>3000</code> milliseconds
     *
     * @param timeout in milliseconds
     * @return config
     */
    public T setTimeout(int timeout) {
        this.timeout = timeout;
        return (T) this;
    }

    public int getTimeout() {
        return timeout;
    }

    /**
     * Setup connection name during connection init
     * via CLIENT SETNAME command
     *
     * @param clientName - name of client
     * @return config
     */
    public T setClientName(String clientName) {
        this.clientName = clientName;
        return (T) this;
    }

    public String getClientName() {
        return clientName;
    }

    @Deprecated
    public T setPingTimeout(int pingTimeout) {
        this.pingTimeout = pingTimeout;
        return (T) this;
    }

    public int getPingTimeout() {
        return pingTimeout;
    }

    /**
     * Timeout during connecting to any Redis server.
     * <p>
     * Default is <code>10000</code> milliseconds.
     * 
     * @param connectTimeout - timeout in milliseconds
     * @return config
     */
    public T setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return (T) this;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * If pooled connection not used for a <code>timeout</code> time
     * and current connections amount bigger than minimum idle connections pool size,
     * then it will closed and removed from pool.
     * <p>
     * Default is <code>10000</code> milliseconds.
     *
     * @param idleConnectionTimeout - timeout in milliseconds
     * @return config
     */
    public T setIdleConnectionTimeout(int idleConnectionTimeout) {
        this.idleConnectionTimeout = idleConnectionTimeout;
        return (T) this;
    }

    public int getIdleConnectionTimeout() {
        return idleConnectionTimeout;
    }

    /*
     * Use setFailedSlaveReconnectionInterval instead
     */
    @Deprecated
    public T setReconnectionTimeout(int slaveRetryTimeout) {
        log.warn("'reconnectionTimeout' setting in unavailable. Please use 'failedSlaveReconnectionInterval' setting instead!");
        return (T) this;
    }

    /*
     * Use setFailedSlaveCheckInterval instead
     */
    @Deprecated
    public T setFailedAttempts(int slaveFailedAttempts) {
        log.warn("'failedAttempts' setting in unavailable. Please use 'failedSlaveCheckInterval' setting instead!");
        return (T) this;
    }
    
    public boolean isSslEnableEndpointIdentification() {
        return sslEnableEndpointIdentification;
    }

    /**
     * Enables SSL endpoint identification.
     * <p>
     * Default is <code>true</code>
     * 
     * @param sslEnableEndpointIdentification - boolean value
     * @return config
     */
    public T setSslEnableEndpointIdentification(boolean sslEnableEndpointIdentification) {
        this.sslEnableEndpointIdentification = sslEnableEndpointIdentification;
        return (T) this;
    }

    public SslProvider getSslProvider() {
        return sslProvider;
    }

    /**
     * Defines SSL provider used to handle SSL connections.
     * <p>
     * Default is JDK
     * 
     * @param sslProvider - ssl provider 
     * @return config
     */
    public T setSslProvider(SslProvider sslProvider) {
        this.sslProvider = sslProvider;
        return (T) this;
    }

    public URL getSslTruststore() {
        return sslTruststore;
    }

    /**
     * Defines path to SSL truststore 
     * 
     * @param sslTruststore - path
     * @return config
     */
    public T setSslTruststore(URL sslTruststore) {
        this.sslTruststore = sslTruststore;
        return (T) this;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    /**
     * Defines password for SSL truststore
     * 
     * @param sslTruststorePassword - password
     * @return config
     */
    public T setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
        return (T) this;
    }

    public URL getSslKeystore() {
        return sslKeystore;
    }

    /**
     * Defines path to SSL keystore
     * 
     * @param sslKeystore - path to keystore
     * @return config
     */
    public T setSslKeystore(URL sslKeystore) {
        this.sslKeystore = sslKeystore;
        return (T) this;
    }

    public String getSslKeystorePassword() {
        return sslKeystorePassword;
    }

    /**
     * Defines password for SSL keystore
     * 
     * @param sslKeystorePassword - password
     * @return config
     */
    public T setSslKeystorePassword(String sslKeystorePassword) {
        this.sslKeystorePassword = sslKeystorePassword;
        return (T) this;
    }

    public int getPingConnectionInterval() {
        return pingConnectionInterval;
    }

    /**
     * Defines PING command sending interval per connection to Redis.
     * <code>0</code> means disable.
     * <p>
     * Default is <code>0</code>
     * 
     * @param pingConnectionInterval - time in milliseconds
     * @return config
     */
    public T setPingConnectionInterval(int pingConnectionInterval) {
        this.pingConnectionInterval = pingConnectionInterval;
        return (T) this;
    }

    public boolean isKeepAlive() {
        return keepAlive;
    }

    /**
     * Enables TCP keepAlive for connection
     * <p>
     * Default is <code>false</code>
     * 
     * @param keepAlive - boolean value
     * @return config
     */
    public T setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
        return (T) this;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Enables TCP noDelay for connection
     * <p>
     * Default is <code>false</code>
     * 
     * @param tcpNoDelay - boolean value
     * @return config
     */
    public T setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return (T) this;
    }

    
    
}
