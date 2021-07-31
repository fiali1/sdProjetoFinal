export ZK=/home/rodrigo98rm/zookeeper
export OUT=/home/rodrigo98rm/Documents/ufabc/2021.2/sd/projeto/out/production/projeto/
export CP_ZK=$OUT:$ZK'/zookeeper-3.4.14.jar':$ZK'/lib/slf4j-log4j12-1.7.25.jar':$ZK'/lib/slf4j-api-1.7.25.jar':$ZK'/lib/log4j-1.2.17.jar'
java -cp $CP_ZK -Dlog4j.configuration=file:$ZK/conf/log4j.properties chat.ZooKeeperClient localhost