/**
 * Copyright (c) 2013-2021 Nikita Koksharov
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
package org.redisson.misc;

import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * 
 * @author Nikita Koksharov
 *
 */
public class RedisURI {

    private final boolean ssl;
    private final String host;
    private final int port;

    public RedisURI(String scheme, String host, int port) {
        this.ssl = "rediss".equals(scheme);
        this.host = host;
        this.port = port;
    }

    public RedisURI(String uri) {
        if (!uri.startsWith("redis://")
                && !uri.startsWith("rediss://")) {
            throw new IllegalArgumentException("Redis url should start with redis:// or rediss:// (for SSL connection)");
        }
        
        String urlHost = uri.replaceFirst("redis://", "http://").replaceFirst("rediss://", "http://");
        String ipV6Host = uri.substring(uri.indexOf("://")+3, uri.lastIndexOf(":"));
        if (ipV6Host.contains("@")) {
            ipV6Host = ipV6Host.split("@")[1];
        }
        if (ipV6Host.contains(":")) {
            urlHost = urlHost.replace(ipV6Host, "[" + ipV6Host + "]");
        }

        try {
            URL url = new URL(urlHost);
            host = url.getHost();
            port = url.getPort();
            ssl = uri.startsWith("rediss://");
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException(e);
        }
    }
    
    public String getScheme() {
        if (ssl) {
            return "rediss";
        }
        return "redis";
    }
    
    public boolean isSsl() {
        return ssl;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
    
    private static String trimIpv6Brackets(String host) {
        if (host.startsWith("[") && host.endsWith("]")) {
            return host.substring(1, host.length() - 1);
        }
        return host;
    }
    
    public static boolean compare(InetSocketAddress entryAddr, RedisURI addr) {
        if (entryAddr == null || addr == null || addr.getHost() == null) {
            return false;
        }

        boolean matchesHostName = entryAddr.getHostName() != null
                && entryAddr.getHostName().equals(trimIpv6Brackets(addr.getHost()));

        boolean matchesHostAddress = entryAddr.getAddress() != null
                && entryAddr.getAddress().getHostAddress() != null
                && entryAddr.getAddress().getHostAddress().equals(trimIpv6Brackets(addr.getHost()));

        return (matchesHostName || matchesHostAddress) && entryAddr.getPort() == addr.getPort();
    }

    @Override
    @SuppressWarnings("AvoidInlineConditionals")
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + port;
        result = prime * result + (ssl ? 1231 : 1237);
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
        RedisURI other = (RedisURI) obj;
        if (host == null) {
            if (other.host != null)
                return false;
        } else if (!host.equals(other.host))
            return false;
        if (port != other.port)
            return false;
        if (ssl != other.ssl)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return getScheme() + "://" + trimIpv6Brackets(host) + ":" + port;
    }
    
}
