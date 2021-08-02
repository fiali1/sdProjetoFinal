package chat;

import org.apache.zookeeper.*;

import java.util.*;

import static chat.ZooKeeperClient.*;
import static org.apache.zookeeper.CreateMode.*;
import static org.apache.zookeeper.ZooDefs.Ids.*;

public class ChatBarrier {
	
	private Chat chat;
	private String barrierPath;
	
	ChatBarrier(Chat chat, String barrierPath) {
		this.chat = chat;
		this.barrierPath = barrierPath;
	}
	
	boolean createBarriersNode() throws InterruptedException, KeeperException {
		return zk.create(barrierPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	void enterBarrier(int participantsCount) throws InterruptedException, KeeperException {
		zk.create(barrierPath + "/" + chat.self + "-", new byte[0], OPEN_ACL_UNSAFE, EPHEMERAL_SEQUENTIAL);
		
		// Listen to new participants joining barrier
		while (true) {
			synchronized (mutex) {
				List<String> barrierParticipants = zk.getChildren(barrierPath, true);
				
				String newestParticipant = "";
				int suffix = Integer.MIN_VALUE;
				
				for (String node : barrierParticipants) {
					int tempIndex = node.lastIndexOf("-");
					int tempSuffix = Integer.parseInt(node.substring(tempIndex + 1));
					
					if (tempSuffix > suffix) {
						suffix = tempSuffix;
						newestParticipant = node.substring(0, tempIndex);
					}
				}
				
				System.out.println(newestParticipant + " just joined! (" + barrierParticipants.size() + "/" + participantsCount + ")");
				
				if (barrierParticipants.size() < participantsCount) mutex.wait();
				else {
					System.out.println("All participants have joined!");
					return;
				}
			}
		}
	}
}
