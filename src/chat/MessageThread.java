package chat;

import org.apache.zookeeper.*;

import java.util.*;

import static chat.ZooKeeperClient.*;

public class MessageThread extends Thread implements Watcher {
	
	Chat chat;
	
	MessageThread(Chat chat) {
		this.chat = chat;
	}
	
	@Override
	public void run() {
		try {
			zk.getChildren(chat.messagesPath + "/" + chat.self.name, true);
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		try {
			processMessage();
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	void processMessage() throws InterruptedException, KeeperException {
		String participantMessagesPath = chat.messagesPath + "/" + chat.self.name;
		List<String> children = zk.getChildren(participantMessagesPath, true);
		String smallestNode = Chat.findSmallestNode(children);
		
		if (!smallestNode.equals("")) {
			String sender = smallestNode.substring(0, smallestNode.lastIndexOf("-"));
			String message = new String(zk.getData(participantMessagesPath + "/" + smallestNode, false, null));
			
			System.out.println("<" + sender + "> " + message);
			
			zk.delete(participantMessagesPath + "/" + smallestNode, 0);
		}
	}
}
