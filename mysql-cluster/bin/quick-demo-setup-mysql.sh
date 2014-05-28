#!/bin/sh

#set -e

MYSQL_ARCHIVE=$1
MYSQL_ARCHIVE_NAME=$(basename $MYSQL_ARCHIVE .tar.gz)
MYSQL_INSTALL_HOME=`pwd`/DEMO/$MYSQL_ARCHIVE_NAME
SCRIPTS_DIR=`pwd`/target/mysql-cluster-pkg/bin
CURR_DIR=`pwd`
DEMO_DIR=`pwd`/DEMO

if [ -z $MYSQL_ARCHIVE ]
then
 echo "Usage: demo.sh <path_to_mysql_archive (tar.gz format)>"
 exit 1
fi
set +x

mkdir -p $DEMO_DIR
# untar helix package
chmod + $SCRIPTS_DIR/*
cd $DEMO_DIR

if [ -d $MYSQL_ARCHIVE_NAME ]
then
 echo "Already installed MySQL"
else 
  echo "Installing MySQL"
  tar -xzvf $MYSQL_ARCHIVE 
fi

if [ ! -d mysql_instances ]
then
  mkdir mysql_instances
fi 

# Setup MySQL
MYSQL_INSTANCE_ROOT=`pwd`/mysql_instances
cd $MYSQL_INSTALL_HOME
for port in 5550 5551; do
  # Make sandboxe
 # make_sandbox $MYSQL_ARCHIVE -- \
  #  --sandbox_directory="localhost_$port" \
  #  --sandbox_port="$port" \
  #  -c "server-id=$port" \
  #  -c "character-set-server=utf8" \
  #  -c "log_bin" \
  #  -c "log-slave-updates=1" \
  #  -c "binlog-format=row" \
  #  -c "gtid-mode=on" \
  #  -c "enforce-gtid-consistency=1" \
  #  -c "innodb-file-per-table=1" \
  #  -c "replicate-ignore-db=mysql" \
  #  -c "binlog-checksum=NONE" \
  # --no_confirm
  instance_name="i-$port"  
  INSTANCE=$MYSQL_INSTANCE_ROOT/$instance_name
  mkdir -p $INSTANCE/{data,etc}
  cat > $INSTANCE/etc/my.cnf << EOF
  [mysqld]
  server-id = $port
  bind-address= 0.0.0.0
  pid-file= mysqld.pid
  log-error= mysqld.err
  character-set-server= utf8
  log-bin=mysql-bin
  log-slave-updates=1
  binlog-format=row
  gtid-mode=on
  enforce-gtid-consistency=1
  innodb-file-per-table=1
  replicate-ignore-db=mysql
  socket=$INSTANCE/mysql.sock 
  explicit_defaults_for_timestamp=1
  
  [client]
  default-character-set= utf8
EOF
  echo "Stopping Mysql instance if any running on port:$port. Error/warning can be ignored"
  bin/mysqladmin -u root --protocol=tcp --port=$port shutdown
  echo "Bootstrapping MySQL instance at $INSTANCE"
  scripts/mysql_install_db --cross-bootstrap --basedir=$MYSQL_INSTALL_HOME --datadir=$INSTANCE/data --defaults-file=$INSTANCE/etc/my.cnf
  MYSQL_UNIX_PORT=$INSTANCE/mysql.sock
  cd $MYSQL_INSTALL_HOME
  echo "Starting MySQL instance"
  bin/mysqld_safe --defaults-file=$INSTANCE/etc/my.cnf --basedir=$MYSQL_INSTALL_HOME --datadir=$INSTANCE/data --port=$port &
  sleep 5
  echo "Checking MySQL connection with user:root"
  bin/mysql -u root --protocol=tcp --port=$port -e "SELECT NOW()"
  echo "Adding super user monty" 
  bin/mysql -u root --protocol=tcp --port=$port -e "GRANT ALL PRIVILEGES ON *.* TO 'monty'@'localhost' IDENTIFIED BY 'some_pass' WITH GRANT OPTION"
  bin/mysql -u root --protocol=tcp --port=$port -e "GRANT ALL PRIVILEGES ON *.* TO 'monty'@'%'         IDENTIFIED BY 'some_pass' WITH GRANT OPTION"
  echo "Checking connection for user: monty, pass:some_pass"
  bin/mysql -u monty --password=some_pass --protocol=tcp --port=$port -e "SELECT NOW()"
  bin/mysql -u monty --password=some_pass --protocol=tcp --port=$port -e "stop slave;reset slave;reset master"
  echo "Setup complete for instance: $INSTANCE, to connect: \n"
  echo "$MYSQL_INSTALL_HOME/bin/mysql -u monty --password=some_pass --protocol=tcp --port=$port"
done



cd $CURR_DIR
