package org.apache.fullmatix.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.ScopedConfigChangeListener;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.spectator.RoutingTableProvider;

/**
 * Given the database, tablename and partitionId - tells the node that is currently hosting it.
 */
public class ConnectionURLProvider implements ScopedConfigChangeListener {

  private String _zkAddress;
  private String _cluster;
  private HelixManager _manager;
  RoutingTableProvider _routingTableProvider = new RoutingTableProvider();
  Map<String, Integer> _dbPartitionToSliceMapping = new ConcurrentHashMap<String, Integer>();

  public ConnectionURLProvider(String zkAddress, String clusterName) {
    _zkAddress = zkAddress;
    _cluster = clusterName;
  }

  public void start() throws Exception {
    _manager =
        HelixManagerFactory.getZKHelixManager(_cluster, "connectionURLProvider",
            InstanceType.SPECTATOR, _zkAddress);
    _manager.connect();
    _manager.addExternalViewChangeListener(_routingTableProvider);
    _manager.addConfigChangeListener(this, ConfigScopeProperty.RESOURCE);
  }

  public void stop() {
    _manager.disconnect();
  }

  /**
   * @param database
   * @param table
   * @param partitionId
   * @return Returns the configuration of the master. instanceconfig.getRecord().getSimpleField(key)
   *         provides the required info to create a jdbc connection. key can be
   *         "mysql_port, mysql_host"
   */
  public InstanceConfig getMaster(String database, String table, int partitionId) {
    String dbPartitionName = database + "_" + partitionId;
    Integer sliceId = _dbPartitionToSliceMapping.get(dbPartitionName);
    if (sliceId != null) {
      List<InstanceConfig> instances =
          _routingTableProvider.getInstances(MySQLConstants.MASTER_SLAVE_RESOURCE_NAME,
              MySQLConstants.MASTER_SLAVE_RESOURCE_NAME + "_" + sliceId, "MASTER");

      if (instances.size() == 1) {
        // TODO: verify that this instance contains this table.
        return instances.get(0);
      }
    }
    return null;
  }
  
  /**
   * @param database
   * @param table
   * @param partitionId
   * @return Returns the configuration of the master. instanceconfig.getRecord().getSimpleField(key)
   *         provides the required info to create a jdbc connection. key can be
   *         "mysql_port, mysql_host"
   */
  public InstanceConfig getMaster(String database, int partitionId) {
    String dbPartitionName = database + "_" + partitionId;
    Integer sliceId = _dbPartitionToSliceMapping.get(dbPartitionName);
    if (sliceId != null) {
      List<InstanceConfig> instances =
          _routingTableProvider.getInstances(MySQLConstants.MASTER_SLAVE_RESOURCE_NAME,
              MySQLConstants.MASTER_SLAVE_RESOURCE_NAME + "_" + sliceId, "MASTER");

      if (instances.size() == 1) {
        // TODO: verify that this instance contains this table.
        return instances.get(0);
      }
    }
    return null;
  }

  /**
   * @param database
   * @param table
   * @param partitionId
   * @return Returns the configuration of the master. instanceconfig.getRecord().getSimpleField(key)
   *         provides the required info to create a jdbc connection. key can be
   *         "mysql_port, mysql_host"
   */
  public List<InstanceConfig> getSlaves(String database, String table, int partitionId) {
    String dbPartitionName = database + "_" + partitionId;
    Integer sliceId = _dbPartitionToSliceMapping.get(dbPartitionName);
    if (sliceId != null) {
      List<InstanceConfig> instances =
          _routingTableProvider.getInstances(MySQLConstants.MASTER_SLAVE_RESOURCE_NAME,
              MySQLConstants.MASTER_SLAVE_RESOURCE_NAME + "_" + sliceId, "SLAVE");
      return instances;
    }
    return null;
  }

  @Override
  public void onConfigChange(List<HelixProperty> configs, NotificationContext context) {
    List<String> databases = new ArrayList<String>();
    Map<String, List<String>> databaseTablesMap = new HashMap<String, List<String>>();
    PropertyKey key = new PropertyKey.Builder(_cluster).resourceConfigs();
    Map<String, HelixProperty> resourceConfigs =
        _manager.getHelixDataAccessor().getChildValuesMap(key);
    HelixAdmin helixAdmin = _manager.getClusterManagmentTool();
    HelixConfigScopeBuilder scopeBuilder =
        new HelixConfigScopeBuilder(ConfigScopeProperty.PARTITION);
    for (String resource : resourceConfigs.keySet()) {
      HelixProperty resourceConfig = resourceConfigs.get(resource);
      String resourceType = resourceConfig.getRecord().getSimpleField("type");
      if ("DATABASE".equalsIgnoreCase(resourceType)) {
        IdealState resourceIdealState = helixAdmin.getResourceIdealState(_cluster, resource);
        for (String dbPartitionName : resourceIdealState.getPartitionSet()) {
          HelixConfigScope scope =
              scopeBuilder.forCluster(_cluster).forResource(resource).forPartition(dbPartitionName)
                  .build();
          String sliceId = _manager.getConfigAccessor().get(scope, "sliceId");
          _dbPartitionToSliceMapping.put(dbPartitionName, Integer.parseInt(sliceId));
        }
        databases.add(resource);
        databaseTablesMap.put(resource, new ArrayList<String>());
      }
    }
  }

  public static void main(String[] args) throws Exception {
    ConnectionURLProvider provider =
        new ConnectionURLProvider("localhost:2181", "mysql-cluster-test");
    provider.start();
    while (true) {
      int partitionId = (int) (Math.random() * 10000) % 6;
      InstanceConfig master = provider.getMaster("MyDB", "MyTable", partitionId);
      if (master != null) {
        System.out.println("Master for partition:" + partitionId + " is " + master.getId());
      } else {
        System.err.println("No master available");
      }
      Thread.sleep(10000);
    }
  }
}
