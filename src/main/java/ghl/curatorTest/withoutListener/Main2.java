package ghl.curatorTest.withoutListener;

import java.util.Collection;
import java.util.Date;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import com.alibaba.fastjson.JSON;

public class Main2 {
	
	public static void main(String[] args){
		 CuratorFramework client;
		 LeaderLatch leaderLatch;
		String zkConnectString = "127.0.0.1:2181";
		String serverId = "LeaderTest1";
		String QUORUM_PATH = "/demo/leader";
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		 client = CuratorFrameworkFactory.builder()
		            .connectString(zkConnectString)
		            .retryPolicy(retryPolicy)
		            .sessionTimeoutMs(30000)
		            .connectionTimeoutMs(30000)
		            .namespace("demo2")
		            .build();
		    client.start();

		    leaderLatch = new LeaderLatch(client, QUORUM_PATH, serverId);
		    try {
				leaderLatch.start();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		    try {
		    	while(true) {
		    		
				Participant leader = leaderLatch.getLeader();
			    Collection<Participant> participants = leaderLatch.getParticipants();

				System.out.println(serverId+" "+new Date()+"Current Participant: "+ JSON.toJSONString(participants));

			 	System.out.println(serverId+" "+new Date()+ "Current Leader: {}"+leader);
			 	
			 	Thread.sleep(3000);
		    	}
			 	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally {
				
				CloseableUtils.closeQuietly(leaderLatch);
				CloseableUtils.closeQuietly(client);

			}
		    
			
	}
}
