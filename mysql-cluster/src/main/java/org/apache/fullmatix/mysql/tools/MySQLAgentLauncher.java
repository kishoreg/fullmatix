package org.apache.fullmatix.mysql.tools;

import java.util.List;

import org.apache.fullmatix.mysql.MySQLAgent;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;

public class MySQLAgentLauncher {
  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.out.println("USAGE: MySQLAgentLauncher zkAddress clusterName instanceName");
    }
    String zkAddress = args[0];
    String clusterName = args[1];
    String instanceName = args[2];
    HelixAdmin admin = new ZKHelixAdmin(zkAddress);
    List<String> clusters = admin.getClusters();
    System.out.println(clusters);
    if (!clusters.contains(clusterName)) {
      System.err.println("Cluster is not setup. Use cluster-admin to create a  new cluster");
      System.exit(1);
    }
    InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      String msg =
          "Agent is not yet configured in the cluster. Use cluster-admin to first configure the new Mysql Agent";
      System.err.println(msg);
      System.exit(1);
    }
    admin.close();
    MySQLAgent agent = new MySQLAgent(zkAddress, clusterName, instanceConfig);
    agent.start();
    Thread.currentThread().join();
  }
}
