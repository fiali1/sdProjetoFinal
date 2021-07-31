package chat;

import org.apache.zookeeper.*;

import java.io.*;
import java.util.*;

public class ZooKeeperClient implements Watcher {
	
	static ZooKeeper zk = null;
	static Integer mutex;
	
	String root;
	
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

		System.out.println("Zk client PROCESS");

		synchronized (mutex) {
			//System.out.println("Process: " + event.getType());
			mutex.notify();
		}
	}
	
	public static void main(String[] args) throws InterruptedException, KeeperException {
		if (args.length >= 1) {
			String zookeeperAddress = args[0];
			ZooKeeperClient zooKeeperClient = new ZooKeeperClient(zookeeperAddress);
			
			Scanner scanner = new Scanner(System.in);
			args = scanner.nextLine().split(" ");
			
			String command = args[0].toLowerCase(Locale.ROOT);
			switch (command) {
				case "new":
					if (args.length >= 3) {
						String name = args[1].toLowerCase(Locale.ROOT);
						int participantsCount = Integer.parseInt(args[2]);
						
						Participant leader = new Participant(name);
						Chat chat = new Chat(scanner, participantsCount, leader);
						while (true) ;
					}
					break;
				case "join":
					if (args.length >= 3) {
						int id = Integer.parseInt(args[1]);
						String name = args[2].toLowerCase(Locale.ROOT);
						Chat chat = Chat.fromId(scanner, id, name);
						while (true) ;
					}
					break;
				default:
					System.out.println("Invalid command " + command + "!");
			}
		}
	}
	
	public static void queueTest(String args[]) {
		Queue q = new Queue(args[1], "/app3");
		
		System.out.println("Input: " + args[1]);
		int i;
		Integer max = new Integer(args[2]);
		
		if (args[3].equals("p")) {
			System.out.println("Producer");
			for (i = 0; i < max; i++)
				try {
					q.produce(10 + i);
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
		} else {
			System.out.println("Consumer");
			
			for (i = 0; i < max; i++) {
				try {
					int r = q.consume();
					System.out.println("Item: " + r);
				} catch (KeeperException e) {
					i--;
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void barrierTest(String args[]) {
		Barrier b = new Barrier(args[1], "/b1", new Integer(args[2]));
		try {
			boolean flag = b.enter();
			System.out.println("Entered barrier: " + args[2]);
			if (!flag) System.out.println("Error when entering the barrier");
		} catch (KeeperException e) {
		
		} catch (InterruptedException e) {
		
		}
		
		// Generate random integer
		Random rand = new Random();
		int r = rand.nextInt(100);
		// Loop for rand iterations
		for (int i = 0; i < r; i++) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			
			}
		}
		try {
			b.leave();
		} catch (KeeperException e) {
		
		} catch (InterruptedException e) {
		
		}
		System.out.println("Left barrier");
	}
	
	public static void lockTest(String args[]) {
		Lock lock = new Lock(args[1], "/lock", new Long(args[2]));
		try {
			boolean success = lock.lock();
			if (success) {
				lock.compute();
			} else {
				while (true) {
					//Waiting for a notification
				}
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}