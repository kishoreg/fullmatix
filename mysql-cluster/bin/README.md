mysql-cluster
===============

mysql-cluster adds sharding, automatic replication and failure detection and handling on top of MySQL 5.6+. 
 
Mysql runs pretty well on a single node, but there are no (free) off the shelf solutions to scale it beyond a single node. Fullmatix (FulliAutomatix) - mysql-cluster is an effort to enable Mysql to be run in a distributed setting.

Note: If the shards are spread across multiple instances, some of the features that come with MySQL will not work. For e.g. Joins and transactions cannot span across multiple shards. Joins and transactions are still possible within a shard. 

#### Role assignment

MySQL instances are divided into multiple groups referred to as slices. Each slice contains the same number of instances. Within each slice, one instance is selected as the Master and the rest will designated as Slave.
Lets say we have 6 mysql instances and we need to have a redundancy factor of 3. We can divide 6 instances into 2 groups of 3. We refer to each group as a slice. Each Slice consists of 3 mysql instances. Among the 3 MySQL instances in each slice, we designate one of them as master.
<p align=center>
<img src="https://github.com/kishoreg/fullmatix/raw/master/docs/images/SliceAssignment.png" alt="MySQL Cluster Role assignment"/>
</p>

#### Replication

<p align=center>
<img src="https://github.com/kishoreg/fullmatix/raw/master/docs/images/replication.png" alt="MySQL Cluster Master Slave Replication"/>
</p>

Slave in each slice automatically detects its Master and sets up the replication. 

#### Failure Handling 

<p align=center>
<img src="https://github.com/kishoreg/fullmatix/raw/master/docs/images/failover.png" alt="MySQL Cluster Master FailOver"/>
</p>

When a Master fails, one of the Slave is promoted to Master. The other slave automatically detects the change in mastership and starts replicating from the new Master. 

#### Recovery

<p align=center>
<img src="https://github.com/kishoreg/fullmatix/raw/master/docs/images/recovery.png" alt="MySQL Cluster Recovery"/>
</p>

When the failed node comes back up, it will detect the current master and sets up replication automatically.

#### Expansion

Not yet supported

#### Creating Database

A database is a logical entity that can be divided into multiple shards. The user can chose the number of shards at the creation time. The number of shards are fixed and cannot be changed later (at least for now). The main reason why the number of shards is different from the number of slice is to facilitate expansion. One can add more slices and the shards will be automatically redistributed to the new slice (not yet implemented). Lets say we have 4 slices and create a database called MyDB with 100 shards, each slice will host 25 shards. The distribution is random but predictable - as long as the number of slices remain the same, shard to slice assignment will not change.

Each shard eventually results in creation of a database in the MySQL. In the above use case, 100 mysql databases will be created, the names will be MyDB_0 ..... MyDB_99.

Creation of database is dynamic, one does not have to bring down the system to add new databases. However one has to wait for databases to be created on the nodes before creating tables.

#### Creating a table

After creating the databases, we can start adding tables to the databases. Tables are also sharded by virtue of being created within a database. For e.g., if we add a table MyTable to MyDB which is divided into 100 shards, we create MyTable table in each of the 100 shards.

Again adding a table is dynamic but one has to wait until the table is successfully created on all the databases. In general the create table command will ensure that. 


### Quick Demo

The quick demo sets up a cluster with two MySQL instances. Starts up two Helix agents that monitors the MySQL instance. The agents join the Helix cluster as Participants. Helix assigns one of them as Master and the other as Slave. The slave automatically sets up the replication from the Master. Clients join the cluster as spectators and watch the external view of the cluster. External View show who is the Master/Slave and where are the database shards located. After the initial set up, the following steps are performed to simulate failure and recovery.

- Client sends 100 writes, we stop the traffic and validate that both nodes have identical data (validates that replication was setup appropriately).
- Disable the current Master (simulates failure), Helix automatically promotes the current Slave to Master.
- Client automatically detects the new Master and sends 200 writes to the new Master.
- We enable the old master and it rejoins the cluster as a Slave, figures out the current Master and sets up replication.
- We stop the traffic and validate that both nodes have identical data. Thus validating failure detection, handling and recovery.
   
Here is the script that does all of the above.

    Download the tar.gz archive from http://dev.mysql.com/downloads/mysql/
    
    git clone git@github.com:kishoreg/fullmatix.git
    mvn clean package -DskipTests
    cd mysql-cluster
    //sets up two MySQL instances that run on port 5550 and 5551
    ./quick-demo-mysql-setup.sh <path to downloaded MySQL tar ball, (must be tar.gz)>
    //sets up Helix cluster, configures the database MyDB with 6 partitions, configures MyTable within MyDB with schema ( col1 int, col2 int). It starts up the MySQL Helix agents that listens to transition commands from Helix and takes appropriate actions on the underlying MySQL Instance.
    ./quick-demo-helix-setup.sh 
    ./quick-demo-run.sh 



