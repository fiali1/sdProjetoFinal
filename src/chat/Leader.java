package chat;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public class Leader extends ZooKeeperClient {
    String leader;
    String id; //Id of the leader
    String pathName;

    /**
     * Constructor of Leader
     *
     * @param address
     * @param name Name of the election node
     * @param leader Name of the leader node
     *
     */
    Leader(String address, String name, String leader, int id) {
        super(address);
        this.root = name;
        this.leader = leader;
        this.id = new Integer(id).toString();
        // Create ZK node name
        if (zk != null) {
            try {
                //Create election znode
                Stat s1 = zk.exists(root, false);
                if (s1 == null) {
                    zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                //Checking for a leader
                Stat s2 = zk.exists(leader, false);
                if (s2 != null) {
                    byte[] idLeader = zk.getData(leader, false, s2);
                    System.out.println("Current leader with id: "+new String(idLeader));
                }

            } catch (KeeperException e) {
                System.out.println("Keeper exception when instantiating queue: " + e.toString());
            } catch (InterruptedException e) {
                System.out.println("Interrupted exception");
            }
        }
    }

    boolean elect() throws KeeperException, InterruptedException{
        this.pathName = zk.create(root + "/n-", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("My path name is: "+pathName+" and my id is: "+id+"!");
        return check();
    }

    boolean check() throws KeeperException, InterruptedException{
        Integer suffix = new Integer(pathName.substring(12));
        while (true) {
            List<String> list = zk.getChildren(root, false);
            Integer min = new Integer(list.get(0).substring(5));
            System.out.println("List: "+list.toString());
            String minString = list.get(0);
            for(String s : list){
                Integer tempValue = new Integer(s.substring(5));
                //System.out.println("Temp value: " + tempValue);
                if(tempValue < min)  {
                    min = tempValue;
                    minString = s;
                }
            }
            System.out.println("Suffix: "+suffix+", min: "+min);
            if (suffix.equals(min)) {
                this.leader();
                return true;
            }
            Integer max = min;
            String maxString = minString;
            for(String s : list){
                Integer tempValue = new Integer(s.substring(5));
                //System.out.println("Temp value: " + tempValue);
                if(tempValue > max && tempValue < suffix)  {
                    max = tempValue;
                    maxString = s;
                }
            }
            //Exists with watch
            Stat s = zk.exists(root+"/"+maxString, this);
            System.out.println("Watching "+root+"/"+maxString);
            //Step 5
            if (s != null) {
                //Wait for notification
                break;
            }
        }
        System.out.println(pathName+" is waiting for a notification!");
        return false;
    }

    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                try {
                    boolean success = check();
                    if (success) {
                        compute();
                    }
                } catch (Exception e) {e.printStackTrace();}
            }
        }
    }

    void leader() throws KeeperException, InterruptedException {
        System.out.println("Become a leader: "+id+"!");
        //Create leader znode
        Stat s2 = zk.exists(leader, false);
        if (s2 == null) {
            zk.create(leader, id.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            zk.setData(leader, id.getBytes(), 0);
        }
    }

    void compute() {
        System.out.println("I will die after 10 seconds!");
        try {
            new Thread().sleep(10000);
            System.out.println("Process "+id+" died!");
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.exit(0);
    }
}