package ghl.curatorTest.withoutListener;

import java.util.Collection;
import java.util.Date;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;

import com.alibaba.fastjson.JSON;

public class LeaderTest1 extends Thread{
	private CuratorFramework client;
	private LeaderLatch leaderLatch;
	private int isLeaderCount = 0;
	private int isSlaveCount = 0;
	String serverId = "LeaderTest1";
	boolean isLeader = false;
	public LeaderTest1(String serverId) {
		// TODO Auto-generated constructor stub
		this.serverId = serverId;
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			setUp();
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while(true) {
			try {
				checkLeader();
				Thread.sleep(3000);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public void setUp() throws Exception {
	    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
	    client = CuratorFrameworkFactory.builder()
	            .connectString(Const.zkConnectString)
	            .retryPolicy(retryPolicy)
	            .sessionTimeoutMs(60000)
	            .connectionTimeoutMs(3000)
	            .namespace(Const.nameSpace)
	            .build();
	    client.start();

	    leaderLatch = new LeaderLatch(client, Const.QUORUM_PATH, serverId);
	    leaderLatch.start();
	}
	public void checkLeader() throws Exception {
	    //首先利用serverId检查自己是否还存在于leaderlatch选举结果集中
	    //考虑网络阻塞，zk数据异常丢失等情况
	    boolean isExist = false;
	    Collection<Participant> participants = leaderLatch.getParticipants();
	    for (Participant participant : participants) {
	        if (serverId.equals(participant.getId())) {
	            isExist = true;
	            break;
	        }
	    }
	    //如果不存在，则重新加入选举
	    if (!isExist) {
	        System.out.println(serverId+":Current server does not exist on zk, reset leaderlatch");
	        leaderLatch.close();
	        leaderLatch = new LeaderLatch(client, Const.QUORUM_PATH, serverId);
	        leaderLatch.start();
	        System.out.println(serverId+":Successfully reset leaderlatch");
	    }

	    //查看当前leader是否是自己
	    //注意，不能用leaderLatch.hasLeadership()因为有zk数据丢失的不确定性
	    //利用serverId对比确认是否主为自己
	    Participant leader = leaderLatch.getLeader();
	    boolean hashLeaderShip = serverId.equals(leader.getId());

		System.out.println(serverId+" "+new Date()+"Current Participant: "+ JSON.toJSONString(participants));
	 	System.out.println(serverId+" "+new Date()+ "Current Leader: {}"+leader);

	    //主从切换缓冲
	    if(hashLeaderShip) {
	        isLeaderCount++;
	        isSlaveCount = 0;
	    } else {
	        isLeaderCount = 0;
	        isSlaveCount ++;
	    }

	    if (isLeaderCount > 3 && !isLeader) {
	    		System.out.println("Currently run as leader");
	    }

	    if (isSlaveCount > 3 && isLeader) {
	    		System.out.println("Currently run as slave");
	    }
	    System.out.println(serverId+" "+"isLeaderCount:"+isLeaderCount+"  isSlaveCount:"+isSlaveCount);
	}
}
