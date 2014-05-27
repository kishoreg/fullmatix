package org.apache.fullmatix.mysql.tools;

import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.fullmatix.mysql.ConnectionURLProvider;
import org.apache.fullmatix.mysql.MySQLConstants;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class QuickDemo {

  private static final Logger LOG = Logger.getLogger(QuickDemo.class);
  private String _zkAddress;
  private String _clusterName;

  public QuickDemo(String zkAddress, String clusterName) {
    _zkAddress = zkAddress;
    _clusterName = clusterName;

  }

  /**
   * Client sends 100 writes, we stop the traffic and validate that both nodes have identical data
   * (validates that replication was setup appropriately).
   * Disable the current Master (simulates failure), Helix automatically promotes the current Slave
   * to Master.
   * Client automatically detects the new Master and sends 100 writes to the new Master.
   * We enable the old master and it rejoins the cluster as a Slave, figures out the current Master
   * and sets up replication.
   * We stop the traffic and validate that both nodes have identical data. Thus validating failure
   * detection, handling and recovery.
   * @throws Exception
   **/
  public boolean run() throws Exception {
    HelixAdmin admin = new ZKHelixAdmin(_zkAddress);
    String database = "MyDB";
    String table = "MyTable";
    int numPartitions = 6;
    Map<String, Connection> connectionMapping = new HashMap<String, Connection>();

    IdealState masterSlaveIS =
        admin.getResourceIdealState(_clusterName, MySQLConstants.MASTER_SLAVE_RESOURCE_NAME);
    IdealState myDatabaseIS = admin.getResourceIdealState(_clusterName, "MyDB");
    IdealState myTableIS = admin.getResourceIdealState(_clusterName, "MyDB.MyTable");

    boolean isHealthy = false;
    do {
      Thread.sleep(1000);
      ExternalView masterSlaveEV =
          admin.getResourceExternalView(_clusterName, MySQLConstants.MASTER_SLAVE_RESOURCE_NAME);
      ExternalView myDatabaseEV = admin.getResourceExternalView(_clusterName, "MyDB");
      ExternalView myTableEV = admin.getResourceExternalView(_clusterName, "MyDB.MyTable");
      Map<String, Integer> masterSlaveStateCountMap = getStateCountMap(masterSlaveEV);
      Map<String, Integer> databaseStateCountMap = getStateCountMap(myDatabaseEV);
      Map<String, Integer> tableStateCountMap = getStateCountMap(myTableEV);
      Integer numMasters = masterSlaveStateCountMap.get("MASTER");
      Integer numSlaves = masterSlaveStateCountMap.get("SLAVE");
      Integer numDBOnline = databaseStateCountMap.get("ONLINE");
      Integer numTableOnline = tableStateCountMap.get("ONLINE");

      isHealthy = (numMasters != null && numMasters == masterSlaveIS.getNumPartitions());
      isHealthy =
          isHealthy
              && (numSlaves != null && numSlaves == masterSlaveIS.getNumPartitions()
                  * (Integer.parseInt(masterSlaveIS.getReplicas()) - 1));
      isHealthy =
          isHealthy
              && (numDBOnline != null && numDBOnline == myDatabaseIS.getNumPartitions()
                  * (Integer.parseInt(myDatabaseIS.getReplicas())));
      isHealthy =
          isHealthy
              && (numTableOnline != null && numTableOnline == myTableIS.getNumPartitions()
                  * (Integer.parseInt(myTableIS.getReplicas())));
      if (isHealthy) {
        LOG.info("Slice info");
        LOG.info(new String(new ZNRecordSerializer().serialize(masterSlaveEV.getRecord())));
        LOG.info("MyDatabase info");
        LOG.info(new String(new ZNRecordSerializer().serialize(myDatabaseEV.getRecord())));
        LOG.info("MyTable info");
        LOG.info(new String(new ZNRecordSerializer().serialize(myTableEV.getRecord())));
      } else {
        LOG.info("numMasters:" + numMasters + ", numSlaves:" + numSlaves + ", numDBOnline:"
            + numDBOnline + " numTableOnline:" + numTableOnline);
      }
    } while (!isHealthy);
    List<String> instances = admin.getInstancesInCluster(_clusterName);
    for (String instance : instances) {
      InstanceConfig instanceConfig = admin.getInstanceConfig(_clusterName, instance);
      String host = instanceConfig.getHostName();
      String port = instanceConfig.getRecord().getSimpleField(MySQLConstants.MYSQL_PORT);
      String userName = instanceConfig.getRecord().getSimpleField(MySQLConstants.MYSQL_SUPER_USER);
      String password =
          instanceConfig.getRecord().getSimpleField(MySQLConstants.MYSQL_SUPER_PASSWORD);
      Connection connection =
          DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "", userName, password);
      connection.setAutoCommit(true);
      connectionMapping.put(instanceConfig.getId(), connection);
    }
    ConnectionURLProvider provider = new ConnectionURLProvider(_zkAddress, _clusterName);
    provider.start();
    verifyDataConsistency(database, table, numPartitions, connectionMapping, instances);
    InstanceConfig currentMasterConfig = provider.getMaster(database, table, 0);
    LOG.info("Inserting 100 rows into current master:" + currentMasterConfig.getInstanceName());
    performWrites(provider, database, table, numPartitions, connectionMapping);
    LOG.info("Verifying data consistency, if replication was setup correctly master and slave must have the identical data:");
    verifyDataConsistency(database, table, numPartitions, connectionMapping, instances);
    LOG.info("Disabling the current master:" + currentMasterConfig.getInstanceName());
    admin.enableInstance(_clusterName, currentMasterConfig.getInstanceName(), false);
    InstanceConfig newMasterConfig = null;
    InstanceConfig oldMasterConfig = currentMasterConfig;
    long timeElapsed = 0;
    do {
      timeElapsed += 1000;
      Thread.sleep(1000);
      newMasterConfig = provider.getMaster(database, table, 0);
      if (timeElapsed > 60 * 1000) {
        String message =
            "New Master was not elected within a minute after disabling:"
                + oldMasterConfig.getInstanceName();
        LOG.info(message);
        throw new Exception(message);
      }
    } while (newMasterConfig == null
        || newMasterConfig.getInstanceName().equals(oldMasterConfig.getInstanceName()));
    LOG.info("New master:" + newMasterConfig.getInstanceName());
    LOG.info("Inserting 100 rows into current master:" + newMasterConfig.getInstanceName());
    performWrites(provider, database, table, numPartitions, connectionMapping);
    LOG.info("Re-enabling the old master:" + oldMasterConfig.getInstanceName()
        + ". It should join as a slave and set up replication with the new master:"
        + newMasterConfig.getInstanceName());
    admin.enableInstance(_clusterName, oldMasterConfig.getInstanceName(), true);
    // give 30 seconds for the old master to catch up
    Thread.sleep(30000);
    LOG.info("Validating that " + oldMasterConfig.getInstanceName() + " setup replication with "
        + newMasterConfig.getInstanceName() + " and has caught up with the new master");
    verifyDataConsistency(database, table, numPartitions, connectionMapping, instances);
    LOG.info("QUICK DEMO COMPLETED");
    return true;
  }

  private Map<String, Integer> getStateCountMap(ExternalView externalView) {
    Map<String, Integer> stateCountMap = new HashMap<String, Integer>();
    if (externalView == null) {
      return stateCountMap;
    }
    for (String slice : externalView.getPartitionSet()) {
      Map<String, String> stateMap = externalView.getStateMap(slice);
      for (String instance : stateMap.keySet()) {
        String state = stateMap.get(instance);
        if (!stateCountMap.containsKey(state)) {
          stateCountMap.put(state, 0);
        }
        stateCountMap.put(state, stateCountMap.get(state) + 1);
      }
    }
    return stateCountMap;
  }

  private boolean verifyDataConsistency(String database, String table, int numPartitions,
      Map<String, Connection> connectionMapping, List<String> instances) throws Exception {
    int[][] countArray = new int[instances.size()][numPartitions];
    // verify the data
    for (int pId = 0; pId < numPartitions; pId++) {
      for (int i = 0; i < instances.size(); i++) {
        String instance = instances.get(i);
        Connection connection = connectionMapping.get(instance);
        String countQuery = String.format("select count(*) from %s_%s.%s", database, pId, table);
        ResultSet rs = connection.createStatement().executeQuery(countQuery);
        if (rs.next()) {
          countArray[i][pId] = rs.getInt(1);
        }
      }
    }
    LOG.info(instances.get(0) + " MyTable row count in each of the 6 partitions: " + Arrays.toString(countArray[0]));
    LOG.info(instances.get(1) + " MyTable row count in each of the 6 partitions: " + Arrays.toString(countArray[1]));
    if (Arrays.equals(countArray[0], countArray[1])) {
      LOG.info("Data verification passed");
      return true;
    } else {
      LOG.info("Data verification failed");
    }
    throw new Exception("Data verification failed");

  }

  private void performWrites(ConnectionURLProvider provider, String database, String table,
      int numPartitions, Map<String, Connection> connectionMapping) throws SQLException {
    for (int i = 0; i < 100; i++) {
      Integer id = (int) (Math.random() * 10000);
      int partitionId = Math.abs(id.hashCode()) % numPartitions;
      InstanceConfig master = provider.getMaster(database, table, partitionId);
      Connection connection = connectionMapping.get(master.getId());
      String insertQuery =
          String.format("insert into %s (%s, %s) VALUES(?,?)", table, "col1", "col2");
      connection.setCatalog(database + "_" + partitionId);
      PreparedStatement prepareStatement = connection.prepareStatement(insertQuery);
      prepareStatement.setInt(1, id);
      prepareStatement.setInt(2, id);
      prepareStatement.execute();
    }

  }

  public static void main(String[] args) throws Exception {
    String zkAddress = "localhost:2199";
    String clusterName = "FULLMATIX_DEMO";
    if (args.length == 2) {
      zkAddress = args[0];
      clusterName = args[1];
    }
    QuickDemo demo = new QuickDemo(zkAddress, clusterName);
    demo.run();

  }
}
