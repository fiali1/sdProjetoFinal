package chat;

import org.apache.zookeeper.*;

import java.util.*;

import static chat.ZooKeeperClient.*;
import static org.apache.zookeeper.CreateMode.*;
import static org.apache.zookeeper.ZooDefs.Ids.*;

public class ChatQueues {
	
	private Chat chat;
	private Scanner scanner;
	String messagesPath;
	
	ChatQueues(Chat chat, Scanner scanner, String messagesPath) {
		this.chat = chat;
		this.scanner = scanner;
		this.messagesPath = messagesPath;
	}
	
	boolean createMessagesNode() throws InterruptedException, KeeperException {
		return zk.create(messagesPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	void writeMessage() throws InterruptedException, KeeperException {
		System.out.println("Insert your message:");
		
		String message = scanner.nextLine();
		
		for (Participant participant : chat.participants) {
			String messagePath = messagesPath + "/" + participant.name + "/" + chat.self.name + "-";
			zk.create(messagePath, message.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT_SEQUENTIAL);
		}
		
		chat.chatLocks.releaseLock();
		chat.chatLeaders.runElection();
	}
	
	void processMessage() throws InterruptedException, KeeperException {
		String participantMessagesPath = messagesPath + "/" + chat.self.name;
		List<String> children = zk.getChildren(participantMessagesPath, false);
		String smallestNode = Chat.findSmallestNode(children);
		
		if (!smallestNode.equals("")) {
			String sender = smallestNode.substring(0, smallestNode.lastIndexOf("-"));
			String message = new String(zk.getData(participantMessagesPath + "/" + smallestNode, false, null));
			
			System.out.println("<" + sender + "> " + message);
			
			zk.delete(participantMessagesPath + "/" + smallestNode, -1);
			zk.getChildren(participantMessagesPath, true);
		}
	}
}
