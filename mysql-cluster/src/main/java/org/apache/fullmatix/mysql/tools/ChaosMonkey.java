package org.apache.fullmatix.mysql.tools;

import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.fullmatix.mysql.MasterStatus;
import org.apache.fullmatix.mysql.MasterStatus.MasterStatusAttribute;
import org.apache.fullmatix.mysql.MySQLAdmin;
import org.apache.fullmatix.mysql.MySQLConstants;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class ChaosMonkey {

  private static final Logger LOG = Logger.getLogger(ChaosMonkey.class);
  private String _zkAddress;
  private String _clusterName;
  private LoadGenerator _generator;
  private HelixAdmin _admin;

  public ChaosMonkey(String zkAddress, String clusterName) {
    _zkAddress = zkAddress;
    _clusterName = clusterName;
    _generator = new LoadGenerator(zkAddress, clusterName);
    _admin = new ZKHelixAdmin(_zkAddress);

  }

  public void run() throws Exception {
    LOG.info("Starting load generator");
    String resourceName = MySQLConstants.MASTER_SLAVE_RESOURCE_NAME;
    IdealState idealState = _admin.getResourceIdealState(_clusterName, resourceName);
    int numSlices = idealState.getNumPartitions();
    boolean isHealthy = true;
    new Thread(_generator).start();
    _generator.pause();
    int iter = 0;

    while (isHealthy) {
      LOG.info("Starting iteration:" + iter++);
      LOG.info("Un-Pausing LoadGenerator");
      _generator.unpause();
      // generate load for a minute
      Thread.sleep(10 * 1000);
      // pick a random slice and disable the master
      int randomSlice = (int) (Math.random() * 1000) % numSlices;
      LOG.info("Randomly selected sliceNum:" + randomSlice + " to simulate failure");
      String currentMaster = null;
      currentMaster = getCurrentMaster(randomSlice);
      LOG.info("Current master for sliceNum:" + randomSlice + " is " + currentMaster);
      if (currentMaster != null) {
        LOG.info("Pausing LoadGenerator");
        _generator.pause();
        String oldMaster = currentMaster;
        // disable the current master
        LOG.info("Disabling the current master:" + currentMaster);
        _admin.enableInstance(_clusterName, currentMaster, false);
        String newMaster = null;
        // wait until there is a new master
        LOG.info("Waiting for new master:");
        do {
          Thread.sleep(1000);
          newMaster = getCurrentMaster(randomSlice);
        } while (newMaster == null || newMaster.equals(oldMaster));
        LOG.info("New master:" + newMaster);
        // start writes on new master
        _generator.unpause();
        // wait for writes to happen on the new master for X seconds
        Thread.sleep(10 * 1000);
        // enable the old master
        LOG.info("Enabling the old master:" + currentMaster);
        _admin.enableInstance(_clusterName, oldMaster, true);
        _generator.pause();
        // wait for it to become slave and catch up
        Thread.sleep(10000);
        LOG.info("Validating cluster health");
        isHealthy = validate(randomSlice);
      } else {
        LOG.error("No master available for slice:" + randomSlice);
        break;
      }
    }
  }

  /**
   * validates that all partitions have same amount of data
   * @param sliceNumber
   * @return
   */
  private boolean validate(int sliceNumber) {
    IdealState idealstate =
        _admin.getResourceIdealState(_clusterName, MySQLConstants.MASTER_SLAVE_RESOURCE_NAME);
    String partitionName = MySQLConstants.MASTER_SLAVE_RESOURCE_NAME + "_" + sliceNumber;
    Map<String, String> instanceStateMap = idealstate.getInstanceStateMap(partitionName);
    Map<String, MasterStatus> masterStatusMap = new HashMap<String, MasterStatus>();
    for (String instance : instanceStateMap.keySet()) {
      InstanceConfig instanceConfig = _admin.getInstanceConfig(_clusterName, instance);
      MySQLAdmin admin = new MySQLAdmin(instanceConfig);
      MasterStatus masterStatus = admin.getMasterStatus();
      masterStatusMap.put(instance, masterStatus);
      admin.close();
    }
    for (String instance1 : instanceStateMap.keySet()) {
      MasterStatus masterStatus1 = masterStatusMap.get(instance1);
      String gtIdSet1 = masterStatus1.getString(MasterStatusAttribute.Executed_Gtid_Set);
      for (String instance2 : instanceStateMap.keySet()) {
        if (!instance1.equals(instance2)) {
          MasterStatus masterStatus2 = masterStatusMap.get(instance2);
          String gtIdSet2 = masterStatus2.getString(MasterStatusAttribute.Executed_Gtid_Set);
          if (!gtIdSet1.equals(gtIdSet2)) {
            LOG.error("Cluster is unhealthy: gtid set of " + instance1 + ":" + gtIdSet1
                + " does not match the gtid set of " + instance2 + ":" + gtIdSet2);
            return false;
          }
        }
      }
    }
    return true;
  }

  public static void main(String[] args) throws Exception {
    String zkAddress = args[0];
    String clusterName = args[1];
    ChaosMonkey monkey = new ChaosMonkey(zkAddress, clusterName);
    monkey.run();
    System.exit(1);
  }

  private String getCurrentMaster(int randomSlice) {
    String resourceName = MySQLConstants.MASTER_SLAVE_RESOURCE_NAME;
    ExternalView resourceExternalView = _admin.getResourceExternalView(_clusterName, resourceName);
    Map<String, String> instanceStateMap =
        resourceExternalView.getStateMap(resourceName + "_" + randomSlice);
    String currentMaster = null;
    for (String instance : instanceStateMap.keySet()) {
      if (instanceStateMap.get(instance).equalsIgnoreCase("MASTER")) {
        currentMaster = instance;
        break;
      }
    }
    return currentMaster;
  }
}
