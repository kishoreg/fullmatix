package org.apache.fullmatix.mysql;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.fullmatix.mysql.MasterSlaveRebalancer;
import org.apache.fullmatix.mysql.MySQLAgent;
import org.apache.fullmatix.mysql.tools.ClusterAdmin;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class MysqlIntegrationTest {
  public static final String DEFAULT_CLUSTER_NAME = "mysql-cluster-test";
  public static final String DEFAULT_RESOURCE_NAME = "MyDB";
  public static final int DEFAULT_PARTITION_NUMBER = 1;
  public static final int DEFAULT_REPLICATION_FACTOR = 2;

  public static final String DEFAULT_STATE_MODEL = "MasterSlave";

  public static final String[] MYSQL_HOSTS = new String[] {
      "localhost", "localhost"
  };
  public static final int[] MYSQL_PORTS = new int[] {
      5550, 5551
  };

  public static List<InstanceConfig> setupCluster(String zkAddr, String clusterName, int numNodes) {
    ZkClient zkclient = null;
    List<InstanceConfig> instanceConfigs = new ArrayList<InstanceConfig>();
    List<String> instanceNames = new ArrayList<String>();
    try {
      HelixAdmin admin = new ZKHelixAdmin(zkAddr);
      ClusterAdmin clusterAdmin = new ClusterAdmin(admin);
      // add cluster, always recreate
      admin.dropCluster(clusterName);
      clusterAdmin.createCluster(clusterName);

      // addNodes
      for (int i = 0; i < numNodes; i++) {
        String port = String.valueOf(MYSQL_PORTS[i]);
        String host = MYSQL_HOSTS[i];
        String serverId = host + "_" + port;
        instanceNames.add(serverId);
        InstanceConfig config = new InstanceConfig(serverId);
        config.setHostName(host);
        config.setPort(port);
        config.setInstanceEnabled(true);
        config.getRecord().setSimpleField(MySQLConstants.MYSQL_PORT, port);
        config.getRecord().setSimpleField(MySQLConstants.MYSQL_SUPER_USER, "monty");
        config.getRecord().setSimpleField(MySQLConstants.MYSQL_SUPER_PASSWORD, "some_pass");
        admin.addInstance(clusterName, config);
        instanceConfigs.add(config);
      }
      // add resource "MasterSlaveAssignment" which maintains the mapping master and slave mappings
      clusterAdmin.doInitialAssignment(clusterName, instanceNames, 2);

      // Create database
      clusterAdmin.createDatabase(clusterName, "MyDB", 6, "");

      // Add a table to the database
      clusterAdmin.createTable(clusterName, "MyDB", "MyTable", " ( col1 INT, col2 INT ) ");

    } finally {
      if (zkclient != null) {
        zkclient.close();
      }
    }
    return instanceConfigs;
  }

  public static void main(String[] args) throws Exception {
    ConsoleAppender ca = new ConsoleAppender();
    ca.setThreshold(Level.INFO);
    ca.setWriter(new OutputStreamWriter(System.out));
    ca.setLayout(new PatternLayout("%-4r [%t] %-5p %c %x - %m%n"));
    Logger.getRootLogger().addAppender(ca);
   
    final String zkAddr = "localhost:2181";
    final int numNodes = 2;
    final String clusterName = DEFAULT_CLUSTER_NAME;
    List<InstanceConfig> instanceConfigs = setupCluster(zkAddr, clusterName, numNodes);
    final Thread[] startMysqlAgents = startMysqlAgents(zkAddr, clusterName, instanceConfigs);
    final HelixManager controller = startController(zkAddr, clusterName);
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

      @Override
      public void run() {
        System.out.println("Executing system shutdown hooks");
        //stop controller
        controller.disconnect();
        //stop the agents
        for(Thread t:startMysqlAgents){
          t.interrupt();
        }
      }
    }));
    if (Boolean.parseBoolean(System.getenv("RUNNING_IN_ECLIPSE")) == true) {
      System.out.println("You're using Eclipse; click in this console and " +
              "press ENTER to call System.exit() and run the shutdown routine.");
      try {
        System.in.read();
      } catch (IOException e) {
        e.printStackTrace();
      }
      System.exit(0);
    }
    Thread.currentThread().join();
  }

  private static HelixManager startController(String zkAddr, String clusterName) {
    HelixManager helixControllerManager =
        HelixControllerMain.startHelixController(zkAddr, clusterName, "localhost_9100",
            HelixControllerMain.STANDALONE);
    return helixControllerManager;
  }

  private static Thread[] startMysqlAgents(final String zkAddress, final String clusterName,
      final List<InstanceConfig> instanceConfigs) {
    Thread[] threads = new Thread[instanceConfigs.size()];
    for (int i = 0; i < instanceConfigs.size(); i++) {
      final InstanceConfig config = instanceConfigs.get(i);
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          MySQLAgent agent = null;
          try {
            agent = new MySQLAgent(zkAddress, clusterName, config);
            agent.start();
          } catch (Exception e) {
            System.err.println("Exception while starting mysqlagent " + e);
            agent.stop();
          }
        }
      });
      threads[i].start();
    }
    return threads;
  }
}
