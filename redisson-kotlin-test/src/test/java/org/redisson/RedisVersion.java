package org.redisson;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Rui Gu (https://github.com/jackygurui)
 */
public class RedisVersion implements Comparable<RedisVersion>{

	private final String fullVersion;
	private final Integer majorVersion;
	private final Integer minorVersion;
	private final Integer patchVersion;

	public RedisVersion(String fullVersion) {
		this.fullVersion = fullVersion;
		Matcher matcher = Pattern.compile("^([\\d]+)\\.([\\d]+)\\.([\\d]+)").matcher(fullVersion);
		matcher.find();
		majorVersion = Integer.parseInt(matcher.group(1));
		minorVersion = Integer.parseInt(matcher.group(2));
		patchVersion = Integer.parseInt(matcher.group(3));
	}

	public String getFullVersion() {
		return fullVersion;
	}

	public int getMajorVersion() {
		return majorVersion;
	}

	public int getMinorVersion() {
		return minorVersion;
	}

	public int getPatchVersion() {
		return patchVersion;
	}

	@Override
	public int compareTo(RedisVersion o) {
		int ma = this.majorVersion.compareTo(o.majorVersion);
		int mi = this.minorVersion.compareTo(o.minorVersion);
		int pa = this.patchVersion.compareTo(o.patchVersion);
		return ma != 0 ? ma : mi != 0 ? mi : pa;
	}

	public int compareTo(String redisVersion) {
		return this.compareTo(new RedisVersion(redisVersion));
	}

	public static int compareTo(String redisVersion1, String redisVersion2) {
		return new RedisVersion(redisVersion1).compareTo(redisVersion2);
	}

}
