==== Compile and run it ====


To run it, follow these steps:

### Step 1: Start ZooKeeper by running "bin/zkServer.sh start" from a copy 
of the distribution package.

### Step 2: Start the master servers.

java -cp .:target/zookeeper-poc.jar:target/lib/*  org.apache.tajo.TajoMasterZkController server1

java -cp .:target/zookeeper-poc.jar:target/lib/*  org.apache.tajo.TajoMasterZkController server2

### Step 3: Start a couple of workers

java -cp .:target/zookeeper-poc.jar:target/lib/*  org.apache.tajo.TajoWorkerZkController worker1

java -cp .:target/zookeeper-poc.jar:target/lib/*  org.apache.tajo.TajoWorkerZkController worker2


Kill server1 and server2 will be the leader.

Workers will be notified via event regarding the master server failover.

TajoWorkers should reconnect with new Master on the new host/port.All that logic can be implemented in TajoWorkerZkController



