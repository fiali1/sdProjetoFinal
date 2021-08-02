package chat;

import org.apache.zookeeper.*;

import java.util.*;

import static chat.ZooKeeperClient.*;
import static org.apache.zookeeper.CreateMode.*;
import static org.apache.zookeeper.ZooDefs.Ids.*;

public class ChatLocks {
	
	private Chat chat;
	String locksPath;
	
	ChatLocks(Chat chat, String locksPath) {
		this.chat = chat;
		this.locksPath = locksPath;
	}
	
	boolean createLocksNode() throws InterruptedException, KeeperException {
		return zk.create(locksPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	boolean checkPreviousLock() throws InterruptedException, KeeperException {
		List<String> list = zk.getChildren(locksPath, false);
		return list.stream().anyMatch(node -> node.startsWith(chat.self));
	}
	
	void createLock() throws KeeperException, InterruptedException {
		zk.create(locksPath + "/" + chat.self + "-", new byte[0], OPEN_ACL_UNSAFE, EPHEMERAL_SEQUENTIAL);
	}
	
	boolean checkLock() throws InterruptedException, KeeperException {
		List<String> list = zk.getChildren(locksPath, false);
		
		String node = Chat.findSmallestNode(list);
		int start = node.lastIndexOf("/");
		int end = node.lastIndexOf("-");
		String nodeName = node.substring(start + 1, end);
		
		if (nodeName.equals(chat.self)) return true;
		
		int max = Integer.MIN_VALUE;
		String maxNode = "";
		
		int nodeSuffix = Integer.parseInt(node.substring(end + 1));
		
		for (String lockNode : list) {
			int tempEnd = lockNode.lastIndexOf("-");
			int tempSuffix = Integer.parseInt(lockNode.substring(tempEnd + 1));
			
			if (tempSuffix > max && tempSuffix < nodeSuffix) {
				max = tempSuffix;
				maxNode = lockNode;
			}
		}
		
		if (!maxNode.equals(""))
			zk.exists(locksPath + "/" + maxNode, chat.processesThread);
		
		return false;
	}
	
	void releaseLock() throws InterruptedException, KeeperException {
		List<String> list = zk.getChildren(locksPath, false);
		String node = Chat.findSmallestNode(list);
		zk.delete(locksPath + "/" + node, 0);
	}
	
	void processLock() throws InterruptedException, KeeperException {
		List<String> locks = zk.getChildren(locksPath, true);
		String smallestNode = Chat.findSmallestNode(locks);
		
		if (!smallestNode.equals("") && checkLock())
			chat.chatQueues.writeMessage();
	}
}
