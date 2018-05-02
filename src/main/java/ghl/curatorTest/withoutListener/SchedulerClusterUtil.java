package ghl.curatorTest.withoutListener;
import java.util.Collection;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

public class SchedulerClusterUtil {
	static org.slf4j.Logger logger = LoggerFactory.getLogger(SchedulerClusterUtil.class);
	public static String getSchedulerLeader() throws Exception {
		String leaderId = "";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.builder().connectString(Const.zkConnectString)
				.retryPolicy(retryPolicy).sessionTimeoutMs(30000).
				connectionTimeoutMs(30000).namespace(Const.nameSpace)
				.build();
		client.start();

		LeaderLatch leaderLatch = new LeaderLatch(client, Const.QUORUM_PATH, Const.queryServerId);
		try {
			leaderLatch.start();
		
			Collection<Participant> participants;
		
			participants = leaderLatch.getParticipants();
			System.out.println("Current Participant: "+ JSON.toJSONString(participants));
			
			Participant leader = leaderLatch.getLeader();
			if(Const.queryServerId.equals(leader.getId())==false) {
				System.out.println("Current Leader: "+leader);
				leaderId =  leader.getId();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new Exception(e);
		}finally {
			CloseableUtils.closeQuietly(leaderLatch);
			CloseableUtils.closeQuietly(client);
		}
		return leaderId;
	}
	public static void main(String[] args){
		while (true) {
			try {
				System.out.println(SchedulerClusterUtil.getSchedulerLeader());
				Thread.sleep(3000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			
		}
		
			
	}
}
