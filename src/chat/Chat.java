package chat;

import org.apache.zookeeper.*;

import java.nio.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static chat.Util.*;
import static chat.ZooKeeperClient.*;
import static org.apache.zookeeper.CreateMode.*;
import static org.apache.zookeeper.ZooDefs.Ids.*;

public class Chat {
	
	int id;
	
	String path;
	String participantsPath;
	
	ChatBarrier chatBarrier;
	ChatLeaders chatLeaders;
	ChatLocks chatLocks;
	ChatQueues chatQueues;
	
	Participant[] participants;
	Participant self;
	
	ProcessesThread processesThread;
	
	/**
	 * Constructor for first participant
	 */
	Chat(Scanner scanner, int participantsCount, Participant self) {
		try {
			path = createChatNode();
			
			participants = new Participant[participantsCount];
			participantsPath = path + "/participants";
			createParticipantsNode();
			
			chatQueues = new ChatQueues(this, scanner, path + "/messages");
			chatQueues.createMessagesNode();
			
			id = Integer.parseInt(path.substring(path.lastIndexOf("-") + 1));
			
			registerParticipantsCount(participantsCount);
			
			this.self = self;
			registerParticipant(self);
			
			System.out.println("Chat joined!");
			System.out.println("ID: " + id);
			
			registerFeatures(true);
			
			participants = reloadParticipants();
			
			startProcessesThread();
			
			chatQueues.writeMessage();
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Constructor for joining participant
	 */
	Chat(Scanner scanner, int id, String path, int participantsCount, Participant self) {
		this.id = id;
		this.path = path;
		
		participants = new Participant[participantsCount];
		participantsPath = path + "/participants";
		
		chatQueues = new ChatQueues(this, scanner, path + "/messages");
		
		try {
			this.self = self;
			registerParticipant(self);
			
			System.out.println("Chat joined!");
			
			registerFeatures(false);
			
			this.participants = reloadParticipants();
			
			startProcessesThread();
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	private boolean checkRootChatNode() throws InterruptedException, KeeperException {
		return zk.exists("/chats", false) != null;
	}
	
	private void createRootChatNode() throws InterruptedException, KeeperException {
		zk.create("/chats", new byte[0], OPEN_ACL_UNSAFE, PERSISTENT);
	}
	
	private String createChatNode() throws InterruptedException, KeeperException {
		if (!checkRootChatNode()) createRootChatNode();
		
		return zk.create("/chats/chat-", new byte[0], OPEN_ACL_UNSAFE, PERSISTENT_SEQUENTIAL);
	}
	
	private void registerParticipantsCount(int participantsCount) throws InterruptedException, KeeperException {
		zk.create(path + "/participantsCount", intToByteArray(participantsCount), OPEN_ACL_UNSAFE, PERSISTENT);
	}
	
	private void createParticipantsNode() throws InterruptedException, KeeperException {
		zk.create(participantsPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT);
	}
	
	private void registerParticipant(Participant participant) throws InterruptedException, KeeperException {
		String participantPath = participantsPath + "/" + participant.name;
		zk.create(participantPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT);
		zk.create(participantPath + "/name", participant.name.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT);
		zk.create(participantPath + "/host", participant.host.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT);
		zk.create(participantPath + "/port", intToByteArray(participant.port), OPEN_ACL_UNSAFE, PERSISTENT);
		
		zk.create(chatQueues.messagesPath + "/" + participant.name, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT);
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
				  int port = byteArrayToInt(zk.getData(participantPath + "/port", false, null));
				
				  return new Participant(name, host, port);
			  } catch (KeeperException | InterruptedException e) {
				  e.printStackTrace();
				  return null;
			  }
		  })
		  .forEach(participant -> currentParticipants[i.getAndIncrement()] = participant);
		
		return currentParticipants;
	}
	
	void registerFeatures(boolean firstParticipant) throws InterruptedException, KeeperException {
		// Cadidatar-se à eleição
		chatLeaders = new ChatLeaders(this, path + "/leaders");
		if (firstParticipant) chatLeaders.createLeadersNode();
		chatLeaders.applyForLeadership();
		
		// Registrar-se para o lock
		chatLocks = new ChatLocks(this, path + "/locks");
		if (firstParticipant) chatLocks.createLocksNode();
		chatLocks.createLock();
		
		// Entrar no barrier
		chatBarrier = new ChatBarrier(this, path + "/barrier");
		if (firstParticipant) chatBarrier.createBarriersNode();
		chatBarrier.enterBarrier(participants.length);
	}
	
	void startProcessesThread() {
		processesThread = new ProcessesThread(this);
		processesThread.start();
		zk.register(processesThread);
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
	
	private static String chatPath(int id) {
		return "/chats/chat-" + ("0000000000" + id).substring(Integer.toString(id).length());
	}
	
	private static boolean checkChatNode(String path) throws InterruptedException, KeeperException {
		return zk.exists(path, false) != null;
	}
	
	static Chat fromId(Scanner scanner, int id, String joiningName) throws InterruptedException, KeeperException {
		String path = chatPath(id);
		if (checkChatNode(path)) {
			int participantsCount = byteArrayToInt(zk.getData(path + "/participantsCount", false, null));
			return new Chat(scanner, id, path, participantsCount, new Participant(joiningName));
		} else {
			System.out.println("Não há um chat com o id " + id + "!");
			Runtime.getRuntime().exit(0);
			return null;
		}
	}
}
