package ghl.curatorTest.listener;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import ghl.curatorTest.withoutListener.Const;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by zhuzs on 2017/4/17.
 */
public class LeaderLatchTest extends Thread {

	String serverId;

	public LeaderLatchTest(String serverId) {
		// TODO Auto-generated constructor stub
		this.serverId = serverId;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		CuratorFramework client = null;
		LeaderLatch leaderLatch = null;
		try {
			client = getClient();
			leaderLatch = new LeaderLatch(client, Const.QUORUM_PATH, serverId);
			leaderLatch.addListener(new LeaderLatchListener() {
				@Override
				public void isLeader() {
					System.out.println(new Date()+" "+serverId + ":I am leader. I am doing jobs!");
				}

				@Override
				public void notLeader() {
					System.out.println(new Date()+serverId + ":I am not leader. I will do nothing!");
				}
			});
			leaderLatch.start();
			//Thread.sleep(Integer.MAX_VALUE);
		} catch (Exception e) {
			e.printStackTrace();
		} 
		finally {
			CloseableUtils.closeQuietly(leaderLatch);
			CloseableUtils.closeQuietly(client);
		}
	}

	private static CuratorFramework getClient() {
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.builder().connectString("127.0.0.1:2181")
				.retryPolicy(retryPolicy).sessionTimeoutMs(60000)
				.connectionTimeoutMs(3000).namespace(Const.nameSpace).build();
		client.start();
		return client;
	}
}
