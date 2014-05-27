package org.apache.fullmatix.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.fullmatix.mysql.MasterStatus.MasterStatusAttribute;
import org.apache.fullmatix.mysql.SlaveStatus.SlaveStatusAttribute;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.log4j.Logger;

public class Replicator extends RoutingTableProvider {
  private static Logger LOG = Logger.getLogger(Replicator.class);
  private InstanceConfig currentMasterConfig;
  static String user = "monty";
  static String password = "some_pass";
  AtomicReference<ReplicationState> replicationState =
      new AtomicReference<Replicator.ReplicationState>(ReplicationState.STOPPED);
  private InstanceConfig _localInstanceConfig;

  enum ReplicationState {
    INITIATED_START,
    INITIATED_STOP,
    STARTING,
    STARTED,
    SWITCHING,
    STOPPING,
    STOPPED
  }

  public Replicator(InstanceConfig instanceConfig) {
    _localInstanceConfig = instanceConfig;
  }

  public void initiateStart() {
    replicationState.set(ReplicationState.INITIATED_START);
    // do nothing here, it correct method will be invoked by ClusterStateObserver
  }

  public void start(InstanceConfig newMasterConfig) throws Exception {
    // LOG.info("Starting replication for ");
    replicationState.set(ReplicationState.STARTING);
    if (newMasterConfig != null) {
      String master = newMasterConfig.getInstanceName();
      if (currentMasterConfig == null
          || !master.equalsIgnoreCase(currentMasterConfig.getInstanceName())) {
        LOG.info("Found new master:" + newMasterConfig.getInstanceName());
        if (currentMasterConfig != null) {
          stop();
        }
        currentMasterConfig = newMasterConfig;
        startReplication(currentMasterConfig);
      } else {
        LOG.info("Already replicating from " + master);
      }
    } else {
      LOG.info("No master found");
    }

  }

  public void startReplication(InstanceConfig masterInstanceConfig) throws Exception {
    String masterHost = masterInstanceConfig.getHostName();
    String masterPort = masterInstanceConfig.getRecord().getSimpleField("mysql_port");
    String slaveHost = _localInstanceConfig.getHostName();
    String slaveMysqlPort = _localInstanceConfig.getPort();
    setupReplication(masterHost, masterPort, slaveHost, slaveMysqlPort);
    replicationState.set(ReplicationState.STARTED);
  }

  private void setupReplication(String masterHost, String masterMysqlPort, String slaveHost,
      String slaveMysqlPort) throws Exception {
    boolean started = true;
    Connection masterConnection = null;
    Connection slaveConnection = null;
    Statement masterStatement = null;
    Statement slaveStatement = null;
    try {
      slaveConnection =
          DriverManager.getConnection("jdbc:mysql://" + slaveHost + ":" + slaveMysqlPort + "",
              user, password);
      slaveStatement = slaveConnection.createStatement();

      masterConnection =
          DriverManager.getConnection("jdbc:mysql://" + masterHost + ":" + masterMysqlPort + "",
              user, password);
      masterStatement = masterConnection.createStatement();
      LOG.info("slave status before stopping replication");
      SlaveStatus slaveStatusBeforeStop =
          new SlaveStatus(slaveStatement.executeQuery("show slave status"));
      LOG.info(slaveStatusBeforeStop);

      // FETCH MASTER STATUS
      LOG.info("master status");
      MasterStatus masterStatus =
          new MasterStatus(masterStatement.executeQuery("show master status"));
      LOG.info(masterStatus);

      // STOP SLAVE
      LOG.info("Stopping slave");
      slaveStatement.execute("stop slave");

      LOG.info("slave status after stopping replication");
      SlaveStatus slaveStatusAfterStop =
          new SlaveStatus(slaveStatement.executeQuery("show slave status"));
      LOG.info(slaveStatusAfterStop);

      // CHANGE MASTER
      LOG.info("starting replication with new master");
      StringBuilder changeMasterCommand = new StringBuilder();
      changeMasterCommand.append("change master to ") //
          .append(" master_host=").append("'").append(masterHost).append("'") // hostname
          .append(",master_port=").append(masterMysqlPort) // port
          .append(",master_user=").append("'").append(user).append("'") // user that has super
                                                                        // privilege to setup
          // replication
          .append(",master_password=").append("'").append(password).append("'") // password
          .append(",master_auto_position=1");
      LOG.info("Running  change master to command:" + changeMasterCommand);
      slaveStatement.execute(changeMasterCommand.toString());
      // START SLAVE
      LOG.info("Successfully ran change master command, executing start slave");
      slaveStatement.execute("start slave");
      SlaveStatus slaveStatusAfterStart =
          new SlaveStatus(slaveStatement.executeQuery("show slave status"));
      LOG.info("slave status after starting replication");
      LOG.info(slaveStatusAfterStart);
      started = true;

    } catch (Exception e) {
      LOG.error(e);
    } finally {
      if (slaveConnection != null) {
        slaveConnection.close();
      }
      if (masterConnection != null) {
        masterConnection.close();
      }
    }
    if (started) {
      LOG.info("Replication started in background");
    } else {
      throw new Exception("Unable to start replicating from :" + masterHost);
    }
  }
  public void initiateStop() {
    replicationState.set(ReplicationState.INITIATED_STOP);
    stop();
  }
  public void stop() {
    if (replicationState.get().equals(ReplicationState.STOPPED)
        || replicationState.get().equals(ReplicationState.STOPPING) ||
        currentMasterConfig ==null) {
      // TODO If state is STOPPING, wait until the status is changed to STOPPED
      return;
    }
    LOG.info("Stopping replication from current master:" + currentMasterConfig.getInstanceName());
    replicationState.set(ReplicationState.STOPPING);
    Connection slaveConnection = null;
    Statement slaveStatement = null;
    String slaveHost = _localInstanceConfig.getHostName();
    String slaveMysqlPort = _localInstanceConfig.getPort();
    try {
      slaveConnection =
          DriverManager.getConnection("jdbc:mysql://" + slaveHost + ":" + slaveMysqlPort + "",
              user, password);
      slaveStatement = slaveConnection.createStatement();

      LOG.info("slave status before stopping replication");
      SlaveStatus slaveStatusBeforeStop =
          new SlaveStatus(slaveStatement.executeQuery("show slave status"));
      LOG.info(slaveStatusBeforeStop);

      // STOP SLAVE
      LOG.info("Stopping slave");
      slaveStatement.execute("stop slave");

      LOG.info("slave status after stopping replication");
      SlaveStatus slaveStatusAfterStop =
          new SlaveStatus(slaveStatement.executeQuery("show slave status"));
      LOG.info(slaveStatusAfterStop);
      currentMasterConfig = null;
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      if (slaveConnection != null) {
        try {
          slaveConnection.close();
        } catch (SQLException e) {
          // ignore
        }
      }
    }
  }

  /*
 * 
 */
  public void catchupAndStop() {
    Connection masterConnection = null;
    Connection slaveConnection = null;
    Statement masterStatement = null;
    Statement slaveStatement = null;
    String slaveHost = _localInstanceConfig.getHostName();
    String slaveMysqlPort = _localInstanceConfig.getPort();

    try {
      slaveConnection =
          DriverManager.getConnection("jdbc:mysql://" + slaveHost + ":" + slaveMysqlPort + "",
              user, password);
      slaveStatement = slaveConnection.createStatement();

      LOG.info("slave status before stopping replication");
      SlaveStatus slaveStatusBeforeStop =
          new SlaveStatus(slaveStatement.executeQuery("show slave status"));
      LOG.info(slaveStatusBeforeStop);

      // STOP SLAVE
      LOG.info("Stopping slave");
      slaveStatement.execute("stop slave");

      LOG.info("slave status after stopping replication");
      SlaveStatus slaveStatusAfterStop =
          new SlaveStatus(slaveStatement.executeQuery("show slave status"));
      LOG.info(slaveStatusAfterStop);

      if (currentMasterConfig != null) {
        String masterHost = currentMasterConfig.getHostName();
        String masterMysqlPort = currentMasterConfig.getPort();
        try {
          masterConnection =
              DriverManager.getConnection(
                  "jdbc:mysql://" + masterHost + ":" + masterMysqlPort + "", user, password);
          masterStatement = masterConnection.createStatement();
          // FETCH MASTER STATUS
          LOG.info("master status");
          MasterStatus masterStatus =
              new MasterStatus(masterStatement.executeQuery("show master status"));
          LOG.info(masterStatus);
          String file = masterStatus.getString(MasterStatusAttribute.File);
          String position = masterStatus.getString(MasterStatusAttribute.Position);
          if (file != null && position != null) {
            StringBuilder catchupQuery = new StringBuilder();
            catchupQuery.append("START SLAVE UNTIL ").append("MASTER_LOG_FILE='").append(file)
                .append("', master_log_pos=").append(position);
            masterStatement.execute(catchupQuery.toString());
            LOG.info("Successfully applied changes from last master");
            SlaveStatus slaveStatusAfterCatchup =
                new SlaveStatus(slaveStatement.executeQuery("show slave status"));
            LOG.info("slave status after catch up");
            LOG.info(slaveStatusAfterCatchup);
            // catch up query returns after the changes are pulled into mysql relay log, wait until
            // the sql thread applies all completed.
            String relayMasterLogFile =
                slaveStatusAfterCatchup.getString(SlaveStatusAttribute.Relay_Master_Log_File);
            int readMasterLogPos =
                slaveStatusAfterCatchup.getInt(SlaveStatusAttribute.Read_Master_Log_Pos);
            int execMasterLogPos =
                slaveStatusAfterCatchup.getInt(SlaveStatusAttribute.Exec_Master_Log_Pos);

            StringBuilder waitQuery = new StringBuilder();
            if (!file.equals(relayMasterLogFile) || execMasterLogPos != readMasterLogPos) {
              waitQuery.append("select MASTER_POS_WAIT('").append(file).append("',")
                  .append(execMasterLogPos).append(")");
              LOG.info("Running MASTER_POS_WAIT query since slave is not caught up completely with master. ");
              LOG.info(String
                  .format(
                      "relayMasterLogFile=%s, masterLogFile=%s, readMasterLogPos=%s, execMasterLogPos=%s",
                      relayMasterLogFile, file, readMasterLogPos, execMasterLogPos));
              slaveStatement.execute(waitQuery.toString());
              SlaveStatus slaveStatus =
                  new SlaveStatus(slaveStatement.executeQuery("show slave status"));
              relayMasterLogFile =
                  slaveStatus.getString(SlaveStatusAttribute.Relay_Master_Log_File);
              readMasterLogPos = slaveStatus.getInt(SlaveStatusAttribute.Read_Master_Log_Pos);
              execMasterLogPos = slaveStatus.getInt(SlaveStatusAttribute.Exec_Master_Log_Pos);
              if (!file.equals(relayMasterLogFile) || execMasterLogPos != readMasterLogPos) {
                LOG.error("Unable to catch up from previous master, some data might be lost. Continuing to become master.");
              }
            }
          }
          currentMasterConfig = null;
        } catch (Exception e) {
          LOG.error("Exception while running catchup phase", e);
        } finally {
          if (masterConnection != null) {
            masterConnection.close();
          }
        }
      }
    } catch (Exception e) {
      LOG.error(e);
    } finally {
      if (slaveConnection != null) {
        try {
          slaveConnection.close();
        } catch (SQLException e) {
          // ignore
        }
      }

    }
  }
}
