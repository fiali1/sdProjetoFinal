package chat;

import org.apache.zookeeper.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

public class ZooKeeperClient implements Watcher {
	
	static ZooKeeper zk = null;
	static Integer mutex;
	
	ZooKeeperClient(String address) {
		if (zk == null) {
			try {
				System.out.println("Connecting to ZooKeeper...");
				
				zk = new ZooKeeper(address, 3000, this);
				
				System.out.println("Connected to ZooKeeper!");
			} catch (IOException e) {
				e.printStackTrace();
				zk = null;
			}
			mutex = -1;
		}
	}
	
	synchronized public void process(WatchedEvent event) {
		synchronized (mutex) {
			mutex.notify();
		}
	}
	
	public static void main(String[] args) throws InterruptedException, KeeperException {
		if (args.length >= 1) {
			String zookeeperAddress = args[0];
			new ZooKeeperClient(zookeeperAddress);
			
			TimeUnit.SECONDS.sleep(1);
			System.out.println("new amount name | join roomId name");
			
			Scanner scanner = new Scanner(System.in);
			args = scanner.nextLine().split(" ");
			
			String command = args[0].toLowerCase(Locale.ROOT);
			switch (command) {
				case "new":
					if (args.length >= 3) {
						int participantsCount = Integer.parseInt(args[1]);
						String name = args[2].toLowerCase(Locale.ROOT);
						new Chat(scanner, participantsCount, name);
						while (true) ;
					}
					break;
				case "join":
					if (args.length >= 3) {
						int id = Integer.parseInt(args[1]);
						String name = args[2].toLowerCase(Locale.ROOT);
						Chat.fromId(scanner, id, name);
						while (true) ;
					}
					break;
				default:
					System.out.println("Invalid command " + command + "!");
			}
		}
	}
}