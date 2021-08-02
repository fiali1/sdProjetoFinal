package chat;

import org.apache.zookeeper.*;

import java.util.*;

import static chat.ZooKeeperClient.*;

public class ProcessesThread extends Thread implements Watcher {
	
	Chat chat;
	
	ProcessesThread(Chat chat) {
		this.chat = chat;
	}
	
	@Override
	public void run() {
		try { // Register first watchers
			// Leader Watcher
			zk.getChildren(chat.chatLeaders.leadersPath, true);
			
			// Lock Watcher
			zk.getChildren(chat.chatLocks.locksPath, true);
			
			// Message Watcher
			zk.getChildren(chat.chatQueues.messagesPath + "/" + chat.self.name, true);
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void process(WatchedEvent event) {
		try {
			if (event.getPath().startsWith(chat.chatLeaders.leadersPath)) chat.chatLeaders.processElection();
			else if (event.getPath().startsWith(chat.chatLocks.locksPath)) chat.chatLocks.processLock();
			else if (event.getPath().startsWith(chat.chatQueues.messagesPath)) chat.chatQueues.processMessage();
			else { // Register watchers again
				// Leader Watcher
				zk.getChildren(chat.chatLeaders.leadersPath, true);
				
				// Locks Watcher
				zk.getChildren(chat.chatLocks.locksPath, true);
				
				// Messages Watcher
				zk.getChildren(chat.chatQueues.messagesPath + "/" + chat.self.name, true);
			}
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
}

