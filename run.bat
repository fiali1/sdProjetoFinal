:: Caminho para o ZooKeeper
set ZK=""

:: Caminho para os arquivos compilados
set OUT=""

set CP_ZK=%OUT%;%ZK%\zookeeper-3.4.14.jar;%ZK%\lib\slf4j-log4j12-1.7.25.jar;%ZK%\lib\slf4j-api-1.7.25.jar;%ZK%\lib\log4j-1.2.17.jar

:: "localhost" define o endereço onde o ZooKeeper está sendo executado
java -cp %CP_ZK% -Dlog4j.configuration=file:%ZK%\conf\log4j.properties chat.ZooKeeperClient localhost
