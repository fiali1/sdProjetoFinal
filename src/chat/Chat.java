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
	
	Participant[] participants;
	int currentParticipants;
	
	Participant leader;
	
	Chat(int id, String path, Participant[] participants, int currentParticipants, Participant leader, Participant joiningParticipant) {
		this.id = id;
		
		this.path = path;
		participantsPath = path + "/participants";
		messagesPath = path + "/messages";
		
		this.participants = participants;
		this.currentParticipants = currentParticipants;
		
		this.leader = leader;
		
		try {
			System.out.println(registerParticipant(joiningParticipant) ? "Chat joined!" : "Couldn't register joining participant!");
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	Chat(int participantsCount, Participant leader) {
		participants = new Participant[participantsCount];
		currentParticipants = 0;
		
		try {
			path = createChatNode();
			participantsPath = path + "/participants";
			messagesPath = path + "/messages";
			id = Integer.parseInt(path.substring(path.lastIndexOf("-") + 1));
			
			registerParticipantsCount(participantsCount);
			
			System.out.println(registerParticipant(leader) ? "Chat created!" : "Couldn't register leader!");
			
			this.leader = leader;
		} catch (InterruptedException | KeeperException e) {
			e.printStackTrace();
		}
	}
	
	private String createChatNode() throws InterruptedException, KeeperException {
		if (!checkRootChatNode()) createRootChatNode();
		
		return zk.create("/chats/chat-", new byte[0], OPEN_ACL_UNSAFE, PERSISTENT_SEQUENTIAL);
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
	
	private boolean checkParticipantsNode() throws InterruptedException, KeeperException {
		return zk.exists(participantsPath, false) != null;
	}
	
	private boolean createParticipantsNode() throws InterruptedException, KeeperException {
		return zk.create(participantsPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	private boolean checkMessagesNode() throws InterruptedException, KeeperException {
		return zk.exists(messagesPath, false) != null;
	}
	
	private boolean createMessagesNode() throws InterruptedException, KeeperException {
		return zk.create(messagesPath, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
	}
	
	private boolean registerParticipant(Participant participant) throws InterruptedException, KeeperException {
		if (!checkParticipantsNode()) createParticipantsNode();
		boolean participantCreated = zk.create(participantsPath + "/" + participant.name, new byte[0], OPEN_ACL_UNSAFE, PERSISTENT) != null;
		
		String participantPath = participantsPath + "/" + participant.name;
		boolean nameCreated = zk.create(participantPath + "/name", participant.name.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT) != null;
		boolean hostCreated = zk.create(participantPath + "/host", participant.host.getBytes(), OPEN_ACL_UNSAFE, PERSISTENT) != null;
		boolean portCreated = zk.create(participantPath + "/port", ByteBuffer.allocate(4).putInt(participant.port).array(), OPEN_ACL_UNSAFE, PERSISTENT) != null;
		
		if (!checkMessagesNode()) createMessagesNode();
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
	
	static Chat fromId(int id, String joiningName) throws InterruptedException, KeeperException {
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
			return new Chat(id, path, currentParticipants, i.get(), leader.get(), new Participant(joiningName));
		} else return null;
	}
}
