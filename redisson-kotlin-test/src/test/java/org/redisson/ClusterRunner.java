package org.redisson;

import org.redisson.misc.BiHashMap;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 *
 * @author Rui Gu (https://github.com/jackygurui)
 */
public class ClusterRunner {

	private final LinkedHashMap<RedisRunner, String> nodes = new LinkedHashMap<>();
	private final LinkedHashMap<String, String> slaveMasters = new LinkedHashMap<>();

	public ClusterRunner addNode(RedisRunner runner) {
		nodes.putIfAbsent(runner, getRandomId());
		if (!runner.hasOption(RedisRunner.REDIS_OPTIONS.CLUSTER_ENABLED)) {
			runner.clusterEnabled(true);
		}
		if (!runner.hasOption(RedisRunner.REDIS_OPTIONS.CLUSTER_NODE_TIMEOUT)) {
			runner.clusterNodeTimeout(5000);
		}
		if (!runner.hasOption(RedisRunner.REDIS_OPTIONS.PORT)) {
			runner.randomPort(1);
			runner.port(RedisRunner.findFreePort());
		}
		if (!runner.hasOption(RedisRunner.REDIS_OPTIONS.BIND)) {
			runner.bind("127.0.0.1");
		}
		return this;
	}

	public ClusterRunner addNode(RedisRunner master, RedisRunner... slaves) {
		addNode(master);
		for (RedisRunner slave : slaves) {
			addNode(slave);
			slaveMasters.put(nodes.get(slave), nodes.get(master));
		}
		return this;
	}

	public synchronized ClusterProcesses run() throws IOException, InterruptedException, RedisRunner.FailedToStartRedisException {
		BiHashMap<String, RedisRunner.RedisProcess> processes = new BiHashMap<>();
		for (RedisRunner runner : nodes.keySet()) {
			List<String> options = getClusterConfig(runner);
			String confFile = runner.dir() + File.separator + nodes.get(runner) + ".conf";
			System.out.println("WRITING CONFIG: for " + nodes.get(runner));
			try (PrintWriter printer = new PrintWriter(new FileWriter(confFile))) {
				options.stream().forEach((line) -> {
					printer.println(line);
					System.out.println(line);
				});
			}
			processes.put(nodes.get(runner), runner.clusterConfigFile(confFile).run());
		}

		boolean allAlive = true;
		for (int i = 0; i < 15; i++) {
			for (RedisRunner.RedisProcess process : processes.valueSet()) {
				allAlive &= process.isAlive();
			}
			if (allAlive) {
				break;
			}
			allAlive = true;
			Thread.sleep(100);
		}
		if (!allAlive) {
			throw new RedisRunner.FailedToStartRedisException();
		}
		Thread.sleep(1000);

		return new ClusterProcesses(processes);
	}

	private List<String> getClusterConfig(RedisRunner runner) {
		String me = runner.getInitialBindAddr() + ":" + runner.getPort();
		List<String> nodeConfig = new ArrayList<>();
		int c = 0;
		for (RedisRunner node : nodes.keySet()) {
			String nodeId = nodes.get(node);
			StringBuilder sb = new StringBuilder();
			String nodeAddr = node.getInitialBindAddr() + ":" + node.getPort();
			sb.append(nodeId).append(" ");
			sb.append(nodeAddr).append(" ");
			sb.append(me.equals(nodeAddr)
					? "myself,"
					: "");
			boolean isMaster = !slaveMasters.containsKey(nodeId);
			if (isMaster) {
				sb.append("master -");
			} else {
				sb.append("slave ").append(slaveMasters.get(nodeId));
			}
			sb.append(" ");
			sb.append("0").append(" ");
			sb.append(me.equals(nodeAddr)
					? "0"
					: "1").append(" ");
			sb.append(c + 1).append(" ");
			sb.append("connected ");
			if (isMaster) {
				sb.append(getSlots(c, nodes.size() - slaveMasters.size()));
				c++;
			}
			nodeConfig.add(sb.toString());
		}
		nodeConfig.add("vars currentEpoch 0 lastVoteEpoch 0");
		return nodeConfig;
	}

	private static String getSlots(int index, int groupNum) {
		final double t = 16383;
		int start = index == 0 ? 0 : (int) (t / groupNum * index);
		int end = index == groupNum - 1 ? (int) t : (int) (t / groupNum * (index + 1)) - 1;
		return start + "-" + end;
	}

	private static String getRandomId() {
		final SecureRandom r = new SecureRandom();
		return new BigInteger(160, r).toString(16);
	}

	public static class ClusterProcesses {
		private final BiHashMap<String, RedisRunner.RedisProcess> processes;

		private ClusterProcesses(BiHashMap<String, RedisRunner.RedisProcess> processes) {
			this.processes = processes;
		}

		public RedisRunner.RedisProcess getProcess(String nodeId) {
			return processes.get(nodeId);
		}

		public String getNodeId(RedisRunner.RedisProcess process) {
			return processes.reverseGet(process);
		}

		public Set<RedisRunner.RedisProcess> getNodes() {
			return processes.valueSet();
		}

		public Set<String> getNodeIds() {
			return processes.keySet();
		}

		public synchronized Map<String, Integer> shutdown() {
			return processes
					.entrySet()
					.stream()
					.collect(Collectors.toMap(
							e -> e.getKey(),
							e -> e.getValue().stop()));
		}
	}
}
