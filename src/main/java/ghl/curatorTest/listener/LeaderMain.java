package ghl.curatorTest.listener;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class LeaderMain {
	public static void main(String[] args) {
		new LeaderLatchTest("10.96.29.30:9090").start();
	}
}
