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
package org.redisson.client;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.ExecutorService;

import org.redisson.config.SslProvider;
import org.redisson.misc.RedisURI;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.resolver.AddressResolverGroup;
import io.netty.util.Timer;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class RedisClientConfig {

    private RedisURI address;
    private InetSocketAddress addr;
    
    private Timer timer;
    private ExecutorService executor;
    private EventLoopGroup group;
    private AddressResolverGroup<InetSocketAddress> resolverGroup;
    private Class<? extends SocketChannel> socketChannelClass = NioSocketChannel.class;
    private int connectTimeout = 10000;
    private int commandTimeout = 10000;
    
    private String password;
    private TlsVersion tlsVersion;
    private int database;
    private String clientName;
    private boolean readOnly;
    private boolean keepPubSubOrder = true;
    private boolean decodeInExecutor;
    private int pingConnectionInterval;
    private boolean keepAlive;
    private boolean tcpNoDelay;
    
    private String sslHostname;
    private boolean sslEnableEndpointIdentification = true;
    private SslProvider sslProvider = SslProvider.JDK;
    private URL sslTruststore;
    private String sslTruststorePassword;
    private URL sslKeystore;
    private String sslKeystorePassword;
    
    public RedisClientConfig() {
    }
    
    RedisClientConfig(RedisClientConfig config) {
        super();
        this.addr = config.addr;
        this.address = config.address;
        this.timer = config.timer;
        this.executor = config.executor;
        this.group = config.group;
        this.socketChannelClass = config.socketChannelClass;
        this.connectTimeout = config.connectTimeout;
        this.commandTimeout = config.commandTimeout;
        this.password = config.password;
        this.tlsVersion = config.tlsVersion;
        this.database = config.database;
        this.clientName = config.clientName;
        this.readOnly = config.readOnly;
        this.keepPubSubOrder = config.keepPubSubOrder;
        this.pingConnectionInterval = config.pingConnectionInterval;
        this.keepAlive = config.keepAlive;
        this.tcpNoDelay = config.tcpNoDelay;
        this.sslEnableEndpointIdentification = config.sslEnableEndpointIdentification;
        this.sslProvider = config.sslProvider;
        this.sslTruststore = config.sslTruststore;
        this.sslTruststorePassword = config.sslTruststorePassword;
        this.sslKeystore = config.sslKeystore;
        this.sslKeystorePassword = config.sslKeystorePassword;
        this.resolverGroup = config.resolverGroup;
        this.sslHostname = config.sslHostname;
        this.decodeInExecutor = config.decodeInExecutor;
    }
    
    public String getSslHostname() {
        return sslHostname;
    }
    public RedisClientConfig setSslHostname(String sslHostname) {
        this.sslHostname = sslHostname;
        return this;
    }

    public RedisClientConfig setAddress(String host, int port) {
        this.address = new RedisURI("redis://" + host + ":" + port);
        return this;
    }
    public RedisClientConfig setAddress(String address) {
        this.address = new RedisURI(address);
        return this;
    }
    public RedisClientConfig setAddress(InetSocketAddress addr, RedisURI address) {
        this.addr = addr;
        this.address = address;
        return this;
    }
    public RedisClientConfig setAddress(RedisURI address) {
        this.address = address;
        return this;
    }
    public RedisURI getAddress() {
        return address;
    }
    public InetSocketAddress getAddr() {
        return addr;
    }
    
    public Timer getTimer() {
        return timer;
    }
    public RedisClientConfig setTimer(Timer timer) {
        this.timer = timer;
        return this;
    }
    
    public ExecutorService getExecutor() {
        return executor;
    }
    public RedisClientConfig setExecutor(ExecutorService executor) {
        this.executor = executor;
        return this;
    }
    
    public EventLoopGroup getGroup() {
        return group;
    }
    public RedisClientConfig setGroup(EventLoopGroup group) {
        this.group = group;
        return this;
    }
    
    public Class<? extends SocketChannel> getSocketChannelClass() {
        return socketChannelClass;
    }
    public RedisClientConfig setSocketChannelClass(Class<? extends SocketChannel> socketChannelClass) {
        this.socketChannelClass = socketChannelClass;
        return this;
    }
    
    public int getConnectTimeout() {
        return connectTimeout;
    }
    public RedisClientConfig setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }
    
    public int getCommandTimeout() {
        return commandTimeout;
    }
    public RedisClientConfig setCommandTimeout(int commandTimeout) {
        this.commandTimeout = commandTimeout;
        return this;
    }
    
    public SslProvider getSslProvider() {
        return sslProvider;
    }
    public RedisClientConfig setSslProvider(SslProvider sslMode) {
        this.sslProvider = sslMode;
        return this;
    }
    
    public URL getSslTruststore() {
        return sslTruststore;
    }
    public RedisClientConfig setSslTruststore(URL sslTruststore) {
        this.sslTruststore = sslTruststore;
        return this;
    }
    
    public URL getSslKeystore() {
        return sslKeystore;
    }
    public RedisClientConfig setSslKeystore(URL sslKeystore) {
        this.sslKeystore = sslKeystore;
        return this;
    }
    
    public String getSslKeystorePassword() {
        return sslKeystorePassword;
    }
    public RedisClientConfig setSslKeystorePassword(String sslKeystorePassword) {
        this.sslKeystorePassword = sslKeystorePassword;
        return this;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }
    public RedisClientConfig setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
        return this;
    }
    
    public boolean isSslEnableEndpointIdentification() {
        return sslEnableEndpointIdentification;
    }
    public RedisClientConfig setSslEnableEndpointIdentification(boolean enableEndpointIdentification) {
        this.sslEnableEndpointIdentification = enableEndpointIdentification;
        return this;
    }

    public String getPassword() {
        return password;
    }
    public RedisClientConfig setPassword(String password) {
        this.password = password;
        return this;
    }

    public TlsVersion getTlsVersion() {
        return tlsVersion;
    }

    public RedisClientConfig setTlsVersion(TlsVersion tlsVersion) {
        this.tlsVersion = tlsVersion;
        return this;
    }
    
    public int getDatabase() {
        return database;
    }
    public RedisClientConfig setDatabase(int database) {
        this.database = database;
        return this;
    }
    
    public String getClientName() {
        return clientName;
    }
    public RedisClientConfig setClientName(String clientName) {
        this.clientName = clientName;
        return this;
    }
    
    public boolean isReadOnly() {
        return readOnly;
    }
    public RedisClientConfig setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    public boolean isKeepPubSubOrder() {
        return keepPubSubOrder;
    }
    public RedisClientConfig setKeepPubSubOrder(boolean keepPubSubOrder) {
        this.keepPubSubOrder = keepPubSubOrder;
        return this;
    }

    public boolean isDecodeInExecutor() {
        return decodeInExecutor;
    }
    public RedisClientConfig setDecodeInExecutor(boolean decodeInExecutor) {
        this.decodeInExecutor = decodeInExecutor;
        return this;
    }

    public int getPingConnectionInterval() {
        return pingConnectionInterval;
    }    
    public RedisClientConfig setPingConnectionInterval(int pingConnectionInterval) {
        this.pingConnectionInterval = pingConnectionInterval;
        return this;
    }
    
    public boolean isKeepAlive() {
        return keepAlive;
    }
    public RedisClientConfig setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }
    public RedisClientConfig setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public AddressResolverGroup<InetSocketAddress> getResolverGroup() {
        return resolverGroup;
    }
    public RedisClientConfig setResolverGroup(AddressResolverGroup<InetSocketAddress> resolverGroup) {
        this.resolverGroup = resolverGroup;
        return this;
    }

    public enum TlsVersion {
        TLSv0("TLSv1"),
        TLSv1("TLSv1.1"),
        TLSv2("TLSv1.2");

        private String tlsVersion;

        public String getTlsVersion() {
            return this.tlsVersion;
        }

        TlsVersion(String tlsVersion) {
            this.tlsVersion = tlsVersion;
        }
    }
    
}
