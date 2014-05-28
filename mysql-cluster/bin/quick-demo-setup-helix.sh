#!/bin/sh

set -e

HELIX_ARCHIVE="http://www.dsgnwrld.com/am/helix/0.6.3/binaries/helix-core-0.6.3-pkg.tar"
HELIX_HOME=`pwd`/DEMO/helix-core-0.6.3
SCRIPTS_DIR=`pwd`/target/mysql-cluster-pkg/bin
ZK_PORT=2199
ZK_ADDRESS="localhost:$ZK_PORT"
CLUSTER_NAME="FULLMATIX_DEMO"
REPLICATION_FACTOR=2
DATABASE_NAME="MyDB"
NUM_PARTITIONS=6
DB_CREATE_SPEC=""
TABLE_NAME="MyTable"
TABLE_SPEC=" ( col1 INT, col2 INT ) "
DEMO_DIR=`pwd`/DEMO


set +x

mkdir -p $DEMO_DIR
# untar helix package
chmod + $SCRIPTS_DIR/*.sh
cd $DEMO_DIR

echo "Checking if HELIX is downloaded and installed"
if [ -d helix-core-0.6.3 ]
then
 echo "Helix already downloaded and installed"
else
 echo "Downloading and installing Helix"
 curl -O $HELIX_ARCHIVE
 tar -xvf helix-core-0.6.3-pkg.tar
fi


# Verify Helix 
if [ -z "$HELIX_HOME" ]; then
  echo "HELIX_HOME not set. Please install Helix and set HELIX_HOME to the installation directory"
  exit 1
fi

cd $DEMO_DIR 

pid=`ps -ef | grep java | grep LocalZKServer | awk '{print $2}'`
if [ ! -z $pid ]
then
  kill -9 $pid
fi

echo "Stopping current processes"

for pid in `ps -ef | egrep "LocalZKServer|HelixControllerMain|MySQLAgentLauncher" | awk '{print $2}'`; do
  if ps -p $pid > /dev/null
  then
    kill -9 $pid
  fi
done

#start zk
echo "Starting zookeeper"
$HELIX_HOME/bin/start-standalone-zookeeper.sh $ZK_PORT 2>&1 > zookeeper.log &

# Make scripts executable
chmod +x $SCRIPTS_DIR/mysql-cluster-admin.sh
chmod +x $SCRIPTS_DIR/start-mysql-agent.sh
chmod +x $SCRIPTS_DIR/quick-demo.sh

##CONFIGURE THE CLUSTER
# Add cluster
echo "Creating cluster:$CLUSTER_NAME"
$SCRIPTS_DIR/mysql-cluster-admin.sh --zookeeperAddress $ZK_ADDRESS --createCluster $CLUSTER_NAME

# Add the mysql agents
echo "Configuring mysql agents for each mysql instance"
for port in 5550 5551; do
  echo "Configuring MySQL agent: localhost:$port"
  $SCRIPTS_DIR/mysql-cluster-admin.sh --zookeeperAddress $ZK_ADDRESS --configureMysqlAgent $CLUSTER_NAME localhost_$port localhost $port monty some_pass
done

#configure master and slave
echo "Setting up Master-Slave topology"
$SCRIPTS_DIR/mysql-cluster-admin.sh --zookeeperAddress $ZK_ADDRESS --rebalanceCluster $CLUSTER_NAME $REPLICATION_FACTOR

# Create the database
echo "Creating a database:$DATABASE_NAME with NUM_PARTITIONS:$NUM_PARTITIONS"
$SCRIPTS_DIR/mysql-cluster-admin.sh --zookeeperAddress $ZK_ADDRESS --createDatabase $CLUSTER_NAME $DATABASE_NAME $NUM_PARTITIONS  \" \"

# Add a table to this database
echo "Creating a table:$TABLE_NAME with SPEC: $TABLE_SPEC in DATABASE:$DATABASE_NAME"
$SCRIPTS_DIR/mysql-cluster-admin.sh --zookeeperAddress $ZK_ADDRESS --createTable $CLUSTER_NAME $DATABASE_NAME $TABLE_NAME "$TABLE_SPEC"

#start the agents
echo "Starting MySQL agents"

for port in 5550 5551; do
  echo "Starting MySQL agent:localhost:$port"
  $SCRIPTS_DIR/start-mysql-agent.sh $ZK_ADDRESS $CLUSTER_NAME localhost_$port 2>&1 > mysql_agent_$port.log & 
done

# Start Helix controller
echo "Starting Helix Controller"
$HELIX_HOME/bin/run-helix-controller.sh --zkSvr $ZK_ADDRESS --cluster $CLUSTER_NAME 2>&1 > helix_controller.log &



