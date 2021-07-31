package chat;

import org.apache.zookeeper.*;

import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static chat.ZooKeeperClient.*;
import static org.apache.zookeeper.CreateMode.*;
import static org.apache.zookeeper.ZooDefs.Ids.*;

public class Chat {
	
	int id;
	
	String path;
	String participantsPath;
	String messagesPath;
	String barrierPath;
	String leadersPath;
	
	Participant[] participants;
	int currentParticipants;
	
	Participant leader;
	Participant self;
	
	ProcessesThread processesThread;
	
	/**
	 * Join constructor
	 *
	 * @param id
	 * @param path
	 * @param participants
	 * @param currentParticipants
	 * @param leader
	 * @param joiningParticipant
	 */
	Chat(Scanner scanner, int id, String path, Participant[] participants, int currentParticipants, Participant leader, Participant joiningParticipant) {
		this.id = id;
		
		this.path = path;
		participantsPath = path + "/participants";
		messagesPath = path + "/messages";
		barrierPath = path + "/barrier";
		leadersPath = path + "/leaders";
		
		this.participants = participants;
		this.currentParticipants = currentParticipants;
		
		this.self = joiningParticipant;
		this.leader = leader;
		
		try {
			boolean registered = registerParticipant(joiningParticipant);
			System.out.println(registered ? "Chat joined!" : "Couldn't register joining participant!");
			applyForLeadership();
			enterBarrier(participants.length);
			
			this.participants = reloadParticipants();
			
			processesThread = new ProcessesThread(this, scanner);
			processesThread.start();
			zk.register(processesThread);
			
			// TODO: Leader Election
			// runElection();
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * New constructor
	 *
	 * @param participantsCount
	 * @param leader
	 */
	Chat(Scanner scanner, int participantsCount, Participant leader) {
		participants = new Participant[participantsCount];
		currentParticipants = 0;
		
		try {
			path = createChatNode();
			participantsPath = path + "/participants";
			messagesPath = path + "/messages";
			barrierPath = path + "/barrier";
			leadersPath = path + "/leaders";
			
			createBarrierNode();
			createLeaderNode();
			createMessagesNode();
			createParticipantsNode();
			
			id = Integer.parseInt(path.substring(path.lastIndexOf("-") + 1));
			
			this.self = leader;
			this.leader = leader;
			
			registerParticipantsCount(participantsCount);
			boolean registered = registerParticipant(leader);
			
			System.out.println(registered ? "Chat created!" : "Couldn't register leader!");
			
			System.out.println("ID: " + id);
			
			System.out.println(this);
			
			applyForLeadership();
			enterBarrier(participantsCount);
			
			participants = reloadParticipants();

			processesThread = new ProcessesThread(this, scanner);
			processesThread.start();
			zk.register(processesThread);
			
			writeMessage(scanner);
			
			// TODO: Leader Election
			// runElection();
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	private String createChatNode() throws InterruptedException, KeeperException {
		if (!checkRootChatNode()) createRootChatNode();
		
		return zk.create("/chats/chat-", new byte[0], OPEN_ACL_UNSAFE, PERSISTENT_SEQUENTIAL);
	}
	
	private boolean createBarrierNode() throws InterruptedException, KeeperException {
		return zk.create(barrierPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	private boolean checkRootChatNode() throws InterruptedException, KeeperException {
		return zk.exists("/chats", false) != null;
	}
	
	private boolean createRootChatNode() throws InterruptedException, KeeperException {
		return zk.create("/chats", new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	private boolean registerParticipantsCount(int participantsCount) throws InterruptedException, KeeperException {
		return zk.create(path + "/participantsCount", ByteBuffer.allocate(4).putInt(participantsCount).array(), OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	private boolean createParticipantsNode() throws InterruptedException, KeeperException {
		return zk.create(participantsPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	private boolean createMessagesNode() throws InterruptedException, KeeperException {
		return zk.create(messagesPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	private boolean createLeaderNode() throws InterruptedException, KeeperException {
		return zk.create(leadersPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	private boolean registerParticipant(Participant participant) throws InterruptedException, KeeperException {
		boolean participantCreated = zk.create(participantsPath + "/" + participant.name, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
		
		String participantPath = participantsPath + "/" + participant.name;
		boolean nameCreated = zk.create(participantPath + "/name", participant.name.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT) != null;
		boolean hostCreated = zk.create(participantPath + "/host", participant.host.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT) != null;
		boolean portCreated = zk.create(participantPath + "/port", ByteBuffer.allocate(4).putInt(participant.port).array(), OPEN_ACL_UNSAFE, PERSISTENT) != null;
		
		boolean messagesCreated = zk.create(messagesPath + "/" + participant.name, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
		
		if (
				participantCreated
						&& nameCreated
						&& hostCreated
						&& portCreated
						&& messagesCreated
		) {
			participants[currentParticipants++] = participant;
			return true;
		} else return false;
	}
	
	private void enterBarrier(int participantsCount) throws InterruptedException, KeeperException {
		zk.create(barrierPath + "/" + self.name, new byte[0], OPEN_ACL_UNSAFE, EPHEMERAL_SEQUENTIAL);
		
		// Listen to new participants joining barrier
		while (true) {
			synchronized (mutex) {
				List<String> barrierParticipants = zk.getChildren(barrierPath, true);
				
				// TODO: Print participant that just joined/entered
				System.out.println(barrierParticipants.size());
				
				if (barrierParticipants.size() < participantsCount) {
					mutex.wait();
				} else {
					System.out.println("All participants have joined!");
					return;
				}
			}
		}
	}
	
	void runElection() throws InterruptedException, KeeperException {
		List<String> list = zk.getChildren(leadersPath, false);
		String node = findSmallestNode(list);

//		System.out.println("Deleting: " + node);

		zk.delete(leadersPath + "/" + node, 0);

		applyForLeadership();
	}
	
	void applyForLeadership() throws KeeperException, InterruptedException {
		zk.create(leadersPath + "/" + self.name + "-", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
		          CreateMode.EPHEMERAL_SEQUENTIAL);
	}
	
	boolean checkLeadership() throws InterruptedException, KeeperException {
		List<String> list = zk.getChildren(leadersPath, false);
		
		String node = findSmallestNode(list);
		
		int start = node.lastIndexOf("/");
		int end = node.lastIndexOf("-");
		String nodeName = node.substring(start + 1, end);
		
//		System.out.println("Novo líder: " + nodeName);

//		if (nodeName.contains(self.name)){
//			System.out.println("I am the new leader!");
//		}
		
		return (nodeName.equals(self.name));
	}
	
	void writeMessage(Scanner scanner) throws InterruptedException, KeeperException {
		System.out.println("Insert your message:");

		String message = scanner.nextLine();

//		System.out.println(Arrays.toString(participants));

		for (Participant participant : participants)
			zk.create(messagesPath + "/" + participant.name + "/" + self.name + "-", message.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT_SEQUENTIAL);

		runElection();
	}
	
	void processMessage() throws InterruptedException, KeeperException {
		String participantMessagesPath = messagesPath + "/" + self.name;
		List<String> children = zk.getChildren(participantMessagesPath, true);
		String smallestNode = findSmallestNode(children);
		
		if (!smallestNode.equals("")) {
			String sender = smallestNode.substring(smallestNode.lastIndexOf("-"));
			String message = new String(zk.getData(participantMessagesPath + "/" + smallestNode, false, null));
			
			System.out.println("<" + sender + "> " + message);
		}
	}
	
	Participant[] reloadParticipants() throws InterruptedException, KeeperException {
		Participant[] currentParticipants = new Participant[participants.length];
		
		AtomicInteger i = new AtomicInteger();
		zk.getChildren(path + "/participants", false)
		  .stream()
		  .map(participantPath -> {
			  participantPath = path + "/participants/" + participantPath;
			  try {
				  String name = new String(zk.getData(participantPath + "/name", false, null));
				  String host = new String(zk.getData(participantPath + "/host", false, null));
				  int port = ByteBuffer.wrap(zk.getData(participantPath + "/port", false, null)).getInt();
				
				  return new Participant(name, host, port);
			  } catch (KeeperException | InterruptedException e) {
				  e.printStackTrace();
				  return null;
			  }
		  })
		  .forEach(participant -> currentParticipants[i.getAndIncrement()] = participant);
		
		return currentParticipants;
	}
	
	@Override
	public String toString() {
		return "Chat{" +
				"id=" + id +
				", path='" + path + '\'' +
				", participantsPath='" + participantsPath + '\'' +
				", messagesPath='" + messagesPath + '\'' +
				", participants=" + Arrays.toString(participants) +
				'}';
	}
	
	private static String chatPath(int id) {
		return "/chats/chat-" + ("0000000000" + id).substring(Integer.toString(id).length());
	}
	
	private static boolean checkChatNode(String path) throws InterruptedException, KeeperException {
		return zk.exists(path, false) != null;
	}
	
	static String findSmallestNode(List<String> list) {
		String nodeName = "";
		int suffix = Integer.MAX_VALUE;
		
		for (String node : list) {
			int index = node.lastIndexOf("-");
			int tempSuffix = Integer.parseInt(node.substring(index + 1));
			
			if (tempSuffix < suffix) {
				suffix = tempSuffix;
				nodeName = node;
			}
		}
		
		return nodeName;
	}
	
	static Chat fromId(Scanner scanner, int id, String joiningName) throws InterruptedException, KeeperException {
		String path = chatPath(id);
		if (checkChatNode(path)) {
			int participantsCount = ByteBuffer.wrap(zk.getData(path + "/participantsCount", false, null)).getInt();
			Participant[] currentParticipants = new Participant[participantsCount];
			
			AtomicReference<Participant> leader = new AtomicReference<>();
			
			AtomicInteger i = new AtomicInteger();
			zk.getChildren(path + "/participants", false)
			  .stream()
			  .map(participantPath -> {
				  participantPath = path + "/participants/" + participantPath;
				  try {
					  String name = new String(zk.getData(participantPath + "/name", false, null));
					  String host = new String(zk.getData(participantPath + "/host", false, null));
					  int port = ByteBuffer.wrap(zk.getData(participantPath + "/port", false, null)).getInt();
					
					  Participant participant = new Participant(name, host, port);
					
					  if (zk.exists(participantPath + "/leader", false) != null) leader.set(participant);
					
					  return participant;
				  } catch (KeeperException | InterruptedException e) {
					  e.printStackTrace();
					  return null;
				  }
			  })
			  .forEach(participant -> currentParticipants[i.getAndIncrement()] = participant);
			return new Chat(scanner, id, path, currentParticipants, i.get(), leader.get(), new Participant(joiningName));
		} else return null;
	}
}
