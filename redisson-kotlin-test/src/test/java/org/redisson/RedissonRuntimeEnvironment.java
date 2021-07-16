package org.redisson;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 *
 * @author Rui Gu (https://github.com/jackygurui)
 */
public class RedissonRuntimeEnvironment {

	public static final boolean isTravis = "true".equalsIgnoreCase(System.getProperty("travisEnv"));
	public static final String redisBinaryPath = System.getProperty("redisBinary", "C:\\redis\\redis-server.exe");
	public static final String tempDir = System.getProperty("java.io.tmpdir");
	public static final String OS;
	public static final boolean isWindows;
	public static final String redisPort;

	static {
		OS = System.getProperty("os.name", "generic");
		isWindows = OS.toLowerCase(Locale.ENGLISH).contains("win");
		String portString = System.getProperty("redisPort", "");
        Pattern portPattern = Pattern.compile("[1-9]\\d+");

        if (portString.isEmpty()) {
            redisPort = "";
        } else if (portPattern.matcher(portString).matches()) {
            redisPort = portString;
        } else {
            String msg = String.format("REDIS RUNNER ENVIRONMENT: Chosen port '%s' does not match pattern '%s'", portString, portPattern.pattern());
            System.out.println(msg);
            redisPort = "";
        }
	}
}
