package chat;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Random;

public class SyncPrimitive implements Watcher {

    static ZooKeeper zk = null;
    static Integer mutex;

    String root;

    SyncPrimitive(String address) {
        if (zk == null) {
            try {
                System.out.println("Starting ZK:");
                zk = new ZooKeeper(address, 3000, this);
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK: " + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
        //else mutex = new Integer(-1);
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            //System.out.println("Process: " + event.getType());
            mutex.notify();
        }
    }

    public static void main(String args[]) {
        if (args[0].equals("qTest"))
            queueTest(args);
        else if (args[0].equals("barrier"))
            barrierTest(args);
        else if (args[0].equals("lock"))
            lockTest(args);
        else
            System.err.println("Unkonw option");
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