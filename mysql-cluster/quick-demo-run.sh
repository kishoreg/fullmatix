#/bin/sh

set -e

SCRIPTS_DIR=`pwd`/target/mysql-cluster-pkg/bin
ZK_PORT=2199
ZK_ADDRESS="localhost:$ZK_PORT"
CLUSTER_NAME="FULLMATIX_DEMO"
DEMO_DIR=`pwd`/DEMO

#Run the quick-demo
$SCRIPTS_DIR/quick-demo.sh $ZK_ADDRESS $CLUSTER_NAME 2>&1 




