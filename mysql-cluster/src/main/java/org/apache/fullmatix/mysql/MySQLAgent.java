package org.apache.fullmatix.mysql;

import java.io.OutputStreamWriter;

import org.apache.helix.HelixConstants.ChangeType;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.CustomCodeInvoker;
import org.apache.helix.participant.HelixCustomCodeRunner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * @author kgopalak
 */
public class MySQLAgent {

  private static final Logger LOG = Logger.getLogger(MySQLAgent.class);

  private String _zkAddress;
  private String _clusterName;
  private InstanceConfig _instanceConfig;
  private String _instanceName;
  private Context _context;

  static {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to load mysql jdbc driver", e);
    }
  }

  public MySQLAgent(String zkAddress, String clusterName, InstanceConfig instanceConfig)
      throws Exception {
    _zkAddress = zkAddress;
    _clusterName = clusterName;
    _instanceConfig = instanceConfig;
    _instanceName = instanceConfig.getInstanceName();
    _context = new Context();
  }

  public void start() throws Exception {

    MySQLAdmin mysqlAdmin = new MySQLAdmin(_instanceConfig);
    Replicator replicator = new Replicator(_instanceConfig);
    HelixManager helixManager =
        HelixManagerFactory.getZKHelixManager(_clusterName, _instanceName,
            InstanceType.PARTICIPANT, _zkAddress);
    _context.setHelixManager(helixManager);
    _context.setMysqlAdmin(mysqlAdmin);
    _context.setReplicator(replicator);
    MasterSlaveTransitionHandlerFactory factory = new MasterSlaveTransitionHandlerFactory(_context);
    helixManager.getStateMachineEngine().registerStateModelFactory("MasterSlave", factory);
    DatabaseTransitionHandlerFactory databaseTransitionHandlerFactory =
        new DatabaseTransitionHandlerFactory(_context);
    helixManager.getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        databaseTransitionHandlerFactory, "DatabaseTransitionHandlerFactory");
    TableTransitionHandlerFactory tableTransitionHandlerFactory =
        new TableTransitionHandlerFactory(_context);
    helixManager.getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        tableTransitionHandlerFactory, "TableTransitionHandlerFactory");
    helixManager.connect();
    ReplicationManager observer = new ReplicationManager(_context);
    helixManager.addExternalViewChangeListener(observer);
    MasterSlaveRebalancer rebalancer = new MasterSlaveRebalancer(_context);
    HelixCustomCodeRunner helixCustomCodeRunner =
        new HelixCustomCodeRunner(helixManager, _zkAddress).invoke(rebalancer)
            .on(ChangeType.CONFIG, ChangeType.LIVE_INSTANCE)
            .usingLeaderStandbyModel("MasterSlave_rebalancer");
    helixCustomCodeRunner.start();
    monitor();
  }

  private void monitor() throws Exception {
    MySQLAdmin mysqlAdmin = _context.getMysqlAdmin();
    HelixAdmin helixAdmin = _context.getHelixManager().getClusterManagmentTool();
    boolean isEnabled = _instanceConfig.getInstanceEnabled();
    int failedPingCount = 0;
    while (true) {
      Thread.sleep(1000);
      if (!mysqlAdmin.ping()) {
        LOG.info("Mysql connection check failed");

        failedPingCount = failedPingCount + 1;
      } else {
        failedPingCount = 0;
        if (!isEnabled) {
          LOG.info("Mysql connection check passed.Enabling the instance");
          helixAdmin.enableInstance(_clusterName, _instanceName, true);
          isEnabled = true;
        }
      }
      if (failedPingCount == 30) {
        LOG.info("Mysql connection check failed 30 times, disabling the instance");
        helixAdmin.enableInstance(_clusterName, _instanceName, false);
        isEnabled = false;
      }
    }
  }

  public void stop() {
    LOG.info("Stopping the MySQL agent. Disconnecting from the cluster");
    _context.getHelixManager().disconnect();
  }

  public static void main(String[] args) throws Exception {
    ConsoleAppender ca = new ConsoleAppender();
    ca.setThreshold(Level.INFO);
    ca.setWriter(new OutputStreamWriter(System.out));
    ca.setLayout(new PatternLayout("%-4r [%t] %-5p %c %x - %m%n"));
    Logger.getRootLogger().addAppender(ca);
    String clusterName = "mysql-cluster-test";
    String instanceName = "kgopalak-ld.linkedin.biz_5555";
    String zkAddress = "localhost:2181";
    // TODO:stand alone way to start the agent
    HelixAdmin admin = new ZKHelixAdmin(zkAddress);
    InstanceConfig instanceConfig = admin.getInstanceConfig(clusterName, instanceName);
    MySQLAgent agent = new MySQLAgent(zkAddress, clusterName, instanceConfig);
    agent.start();
    Thread.currentThread().join();
  }
}
