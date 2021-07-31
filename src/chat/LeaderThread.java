package chat;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.List;
import java.util.Scanner;

import static chat.ZooKeeperClient.zk;

public class LeaderThread extends Thread implements Watcher {

    Chat chat;
    Scanner scanner;

    LeaderThread(Chat chat, Scanner scanner) {
        this.chat = chat;
        this.scanner = scanner;
    }

    @Override
    public void run() {
        try {
            zk.getChildren(chat.leadersPath, true);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {

        System.out.println(event);

        try {
            if(event.getPath().startsWith(chat.leadersPath)) {
                processElection();
            } else {
                zk.getChildren(chat.leadersPath, true);
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
}
