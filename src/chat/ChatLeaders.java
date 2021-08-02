package chat;

import org.apache.zookeeper.*;

import java.util.*;

import static chat.ZooKeeperClient.*;
import static org.apache.zookeeper.CreateMode.*;
import static org.apache.zookeeper.ZooDefs.Ids.*;

public class ChatLeaders {
	
	private Chat chat;
	String leadersPath;
	
	ChatLeaders(Chat chat, String leadersPath) {
		this.chat = chat;
		this.leadersPath = leadersPath;
	}
	
	boolean createLeadersNode() throws InterruptedException, KeeperException {
		return zk.create(leadersPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	void runElection() throws InterruptedException, KeeperException {
		List<String> list = zk.getChildren(leadersPath, false);
		String node = Chat.findSmallestNode(list);
		zk.delete(leadersPath + "/" + node, 0);
		
		applyForLeadership();
	}
	
	void applyForLeadership() throws KeeperException, InterruptedException {
		zk.create(leadersPath + "/" + chat.self + "-", new byte[0], OPEN_ACL_UNSAFE, EPHEMERAL_SEQUENTIAL);
	}
	
	boolean checkLeadership() throws InterruptedException, KeeperException {
		List<String> list = zk.getChildren(leadersPath, false);
		
		String node = Chat.findSmallestNode(list);
		int start = node.lastIndexOf("/");
		int end = node.lastIndexOf("-");
		String nodeName = node.substring(start + 1, end);
		
		return (nodeName.equals(chat.self));
	}
	
	void processElection() throws InterruptedException, KeeperException {
		List<String> children = zk.getChildren(leadersPath, true);
		String smallestNode = Chat.findSmallestNode(children);
		
		if (!smallestNode.equals("") && checkLeadership() && !chat.chatLocks.checkPreviousLock())
			chat.chatLocks.createLock();
	}
}
