package org.apache.fullmatix.mysql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.ConfigChangeListener;
import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceConfigChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

/**
 * Observes the state of the cluster and setup the replication channels with the appropriate master
 */
public class ReplicationManager implements ExternalViewChangeListener, InstanceConfigChangeListener {

  private static Logger LOG = Logger.getLogger(ReplicationManager.class);
  private HelixManager _manager;
  private HelixAdmin _helixAdmin;
  private String _instanceName;
  private String _clusterName;
  private InstanceConfig _instanceConfig;
  private Replicator _replicator;

  public ReplicationManager(Context context) {
    _manager = context.getHelixManager();
    _replicator = context.getReplicator();
    _instanceName = _manager.getInstanceName();
    _clusterName = _manager.getClusterName();
    _helixAdmin = _manager.getClusterManagmentTool();
  }

  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
      NotificationContext changeContext) {
    handle();
  }

  @Override
  public void onInstanceConfigChange(List<InstanceConfig> configs, NotificationContext changeContext) {
    handle();
  }

  private void handle() {
    InstanceConfig instanceConfig = findMaster();
    try {
      if (instanceConfig != null
          && !_instanceName.equalsIgnoreCase(instanceConfig.getInstanceName())) {
        _replicator.start(instanceConfig);
      }
    } catch (Exception e) {
      LOG.error(e);
    }
  }

  private InstanceConfig findMaster() {
    String clusterName = _manager.getClusterName();
    HelixAdmin helixAdmin = _manager.getClusterManagmentTool();
    List<String> resources = helixAdmin.getResourcesInCluster(clusterName);
    // instance, state, partition
    Map<String, Map<String, Set<String>>> instancePartitionStateMapping =
        new HashMap<String, Map<String, Set<String>>>();

    for (String resource : resources) {
      IdealState resourceIdealState = helixAdmin.getResourceIdealState(clusterName, resource);
      if (resourceIdealState == null
          || !resourceIdealState.getStateModelDefRef().equals("MasterSlave")) {
        continue;
      }
      ExternalView externalView = helixAdmin.getResourceExternalView(clusterName, resource);
      if (externalView == null) {
        continue;
      }
      Set<String> partitionSet = externalView.getPartitionSet();
      for (String partition : partitionSet) {
        Map<String, String> stateMap = externalView.getStateMap(partition);
        for (String instance : stateMap.keySet()) {
          if (!instancePartitionStateMapping.containsKey(instance)) {
            instancePartitionStateMapping.put(instance, new HashMap<String, Set<String>>());
          }
          Map<String, Set<String>> map = instancePartitionStateMapping.get(instance);
          String state = stateMap.get(instance);
          if (!map.containsKey(state)) {
            map.put(state, new HashSet<String>());
          }
          map.get(state).add(partition);
        }
      }
    }
    String masterInstance = null;
    Map<String, Set<String>> map = instancePartitionStateMapping.get(_instanceName);
    if (map != null && map.size() == 1 && map.keySet().iterator().next().equals("SLAVE")) {
      Set<String> slavePartitions = map.get("SLAVE");
      // find the master
      for (String instance : instancePartitionStateMapping.keySet()) {
        Set<String> masterPartitions = instancePartitionStateMapping.get(instance).get("MASTER");
        if (slavePartitions.equals(masterPartitions)) {
          masterInstance = instance;
          break;
        }
      }
    }
    if (masterInstance != null) {
      return helixAdmin.getInstanceConfig(clusterName, masterInstance);
    }
    return null;
  }
}
