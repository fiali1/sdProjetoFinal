package chat;

        import org.apache.zookeeper.KeeperException;
        import org.apache.zookeeper.WatchedEvent;
        import org.apache.zookeeper.Watcher;

        import java.util.List;
        import java.util.Scanner;

        import static chat.ZooKeeperClient.zk;

public class ProcessesThread extends Thread implements Watcher {

    Chat chat;
    Scanner scanner;

    ProcessesThread(Chat chat, Scanner scanner) {
        this.chat = chat;
        this.scanner = scanner;
    }

    @Override
    public void run() {
        try {
            // Leader Watcher
            zk.getChildren(chat.leadersPath, true);

            // Message Watcher
            zk.getChildren(chat.messagesPath + "/" + chat.self.name, true);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            if (event.getPath().startsWith(chat.leadersPath)) {
//                System.out.println("Leader process: " + event);

                processElection();
            } else if (event.getPath().startsWith(chat.messagesPath)) {

//                System.out.println("Message process: " + event);
//                System.out.println("event path: " + event.getPath());
//                System.out.println("chat messages path: " + chat.messagesPath);
//                System.out.println("class: " + getClass().getSimpleName());

                processMessage();
            } else {
//                System.out.println("Exception process: " + event);

                // Leader Watcher
                zk.getChildren(chat.leadersPath, true);

                // Messages Watcher
                zk.getChildren(chat.messagesPath + "/" + chat.self.name, true);
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    void processElection() throws InterruptedException, KeeperException {
        String leadersPath = chat.leadersPath;
        List<String> children = zk.getChildren(leadersPath, true);
        String smallestNode = Chat.findSmallestNode(children);

        if (!smallestNode.equals("")) {

            if(chat.checkLeadership()) {
                chat.writeMessage(scanner);
            }
        }
    }

    void processMessage() throws InterruptedException, KeeperException {
//        System.out.println(chat.self.name + " PROCESS MESSAGE");

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

