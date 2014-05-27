package org.apache.fullmatix.mysql;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.fullmatix.mysql.SlaveStatus.SlaveStatusAttribute;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.participant.CustomCodeCallbackHandler;
import org.apache.log4j.Logger;

/**
 * Computes the idealstate for the slices.
 * @author kgopalak
 */
public class MasterSlaveRebalancer implements CustomCodeCallbackHandler {

  private static final Logger LOG = Logger.getLogger(MasterSlaveRebalancer.class);
  private Context _context;

  public MasterSlaveRebalancer(Context context) {
    _context = context;
  }

  @Override
  public void onCallback(NotificationContext context) {
    LOG.info("START: MasterSlaveRebalancer.onCallback running at "+ _context.getHelixManager().getInstanceName());

    if (context.getType().equals(NotificationContext.Type.FINALIZE)) {
      LOG.info("END: MasterSlaveRebalancer.onCallback FINALIZE callback invoked. Likely lost connection to Helix");
      return;
    }

    HelixManager manager = context.getManager();
    String clusterName = manager.getClusterName();
    HelixAdmin helixAdmin = manager.getClusterManagmentTool();
    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, MySQLConstants.MASTER_SLAVE_RESOURCE_NAME);

    if (idealState == null) {
      LOG.info("END: MasterSlaveRebalancer.onCallback. " + MySQLConstants.MASTER_SLAVE_RESOURCE_NAME + " is not yet created");

    }

    PropertyKey.Builder builder = new PropertyKey.Builder(clusterName);
    Map<String, LiveInstance> liveInstancesMap =
        manager.getHelixDataAccessor().getChildValuesMap(builder.liveInstances());

    Map<String, InstanceConfig> instanceConfigs =
        manager.getHelixDataAccessor().getChildValuesMap(builder.instanceConfigs());

    IdealState newIdealState = new IdealState(idealState.getId());
    newIdealState.getRecord().setSimpleFields(idealState.getRecord().getSimpleFields());
    newIdealState.getRecord().setListFields(idealState.getRecord().getListFields());
    for (String partition : idealState.getPartitionSet()) {
      Map<String, String> instanceStateMap = idealState.getInstanceStateMap(partition);
      String currMaster = null;
      Set<String> slaveSet = new TreeSet<String>();
      for (String instance : instanceStateMap.keySet()) {
        if ("MASTER".equalsIgnoreCase(instanceStateMap.get(instance))) {
          currMaster = instance;
        }
        if ("SLAVE".equalsIgnoreCase(instanceStateMap.get(instance))) {
          slaveSet.add(instance);
        }
      }
      String newMaster = currMaster;
      if (!liveInstancesMap.containsKey(currMaster)
          || !instanceConfigs.get(currMaster).getInstanceEnabled()) {
        // need to find a new master.
        newMaster = findNewMaster(liveInstancesMap, instanceConfigs, currMaster, slaveSet);
      } 
      for (String instance : instanceStateMap.keySet()) {
        if (instance.equalsIgnoreCase(newMaster)) {
          newIdealState.setPartitionState(partition, instance, "MASTER");
        } else {
          newIdealState.setPartitionState(partition, instance, "SLAVE");
        }
      }
    }
    if (!idealState.equals(newIdealState)) {
      LOG.info("New idealstate computed.");
      LOG.info(newIdealState.toString());
      manager.getClusterManagmentTool().setResourceIdealState(clusterName, MySQLConstants.MASTER_SLAVE_RESOURCE_NAME,
          newIdealState);
    } else {
      LOG.info("No change in IdealState");
    }
    LOG.info("END: MasterSlaveRebalancer.onCallback");

  }

  private String findNewMaster(Map<String, LiveInstance> liveInstancesMap,
      Map<String, InstanceConfig> instanceConfigs, String prevMasterHost, Set<String> slaveSet) {
    String newMaster = null;
    SlaveStatus slaveStatusOfNewMaster = null;
    for (String slave : slaveSet) {
      if (liveInstancesMap.containsKey(slave) && instanceConfigs.get(slave).getInstanceEnabled()) {
        MySQLAdmin admin = new MySQLAdmin(instanceConfigs.get(slave));
        SlaveStatus slaveStatus = admin.getSlaveStatus();
        if (slaveStatus != null) {
          String masterHost = slaveStatus.getString(SlaveStatusAttribute.Master_Host);
          String masterPort = slaveStatus.getString(SlaveStatusAttribute.Master_Port);
          String slaveIOStatus = slaveStatus.getString(SlaveStatusAttribute.Slave_IO_Running);
          String slaveSQLStatus = slaveStatus.getString(SlaveStatusAttribute.Slave_SQL_Running);
          String masterLogFile = slaveStatus.getString(SlaveStatusAttribute.Master_Log_File);
          int readMasterLogPos = slaveStatus.getInt(SlaveStatusAttribute.Read_Master_Log_Pos);
          if ((masterHost + "_" + masterPort).equals(prevMasterHost)
              && "YES".equalsIgnoreCase(slaveIOStatus) && "YES".equalsIgnoreCase(slaveSQLStatus)) {
            if (newMaster == null) {
              newMaster = slave;
              slaveStatusOfNewMaster = slaveStatus;
            } else {
              boolean isThisSlaveAhead =
                  masterLogFile.compareTo(slaveStatusOfNewMaster
                      .getString(SlaveStatusAttribute.Master_Log_File)) > 0
                      && readMasterLogPos > slaveStatusOfNewMaster
                          .getInt(SlaveStatusAttribute.Read_Master_Log_Pos);
              if (isThisSlaveAhead) {
                newMaster = slave;
                slaveStatusOfNewMaster = slaveStatus;
              }
            }
          }
        }
      }
    }
    return newMaster;
  }

  
}
