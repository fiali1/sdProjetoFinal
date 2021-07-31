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

		System.out.println(event);

		System.out.println("event path: " + event.getPath());
		System.out.println("chat messages path: " + chat.messagesPath);
		System.out.println("class: " + getClass().getSimpleName());

		try {
			if(event.getPath().startsWith(chat.messagesPath)) {
				processMessage();
			} else {
				zk.getChildren(chat.messagesPath + "/" + chat.self.name, true);
			}
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	void processMessage() throws InterruptedException, KeeperException {
		System.out.println(chat.self.name + " PROCESS MESSAGE");

		String participantMessagesPath = chat.messagesPath + "/" + chat.self.name;
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
