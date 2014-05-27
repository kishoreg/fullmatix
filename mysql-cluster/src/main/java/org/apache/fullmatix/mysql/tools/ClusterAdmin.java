package org.apache.fullmatix.mysql.tools;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.fullmatix.mysql.MySQLAdmin;
import org.apache.fullmatix.mysql.MySQLConstants;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.builder.AutoModeISBuilder;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.log4j.Logger;

public class ClusterAdmin {

  private static final Logger LOG = Logger.getLogger(ClusterAdmin.class);
  private HelixAdmin _helixAdmin;
  private static String createCluster = "createCluster";
  private static String createDatabase = "createDatabase";
  private static String createTable = "createTable";
  private static String configureMysqlAgent = "configureMysqlAgent";
  private static String rebalanceCluster = "rebalanceCluster";

  public ClusterAdmin(HelixAdmin helixAdmin) {
    _helixAdmin = helixAdmin;
  }

  public void createCluster(String clusterName) {
    _helixAdmin.addCluster(clusterName);
    ZNRecord masterSlave = StateModelConfigGenerator.generateConfigForMasterSlave();
    _helixAdmin.addStateModelDef(clusterName, masterSlave.getId(), new StateModelDefinition(
        masterSlave));
    ZNRecord onlineOffline = StateModelConfigGenerator.generateConfigForOnlineOffline();
    _helixAdmin.addStateModelDef(clusterName, onlineOffline.getId(), new StateModelDefinition(
        onlineOffline));
    ZNRecord leaderStandby = StateModelConfigGenerator.generateConfigForLeaderStandby();
    _helixAdmin.addStateModelDef(clusterName, leaderStandby.getId(), new StateModelDefinition(
        leaderStandby));
  }

  public void addMysqlAgent(String clusterName, String agentName, String mysqlHost,
      String mysqlPort, String mysqlSuperUser, String mysqlPassword) {
    InstanceConfig config = new InstanceConfig(agentName);
    config.setHostName(mysqlHost);
    config.setPort(mysqlPort);
    config.getRecord().setSimpleField(MySQLConstants.MYSQL_HOST, mysqlHost);
    config.getRecord().setSimpleField(MySQLConstants.MYSQL_PORT, mysqlPort);
    config.getRecord().setSimpleField(MySQLConstants.MYSQL_SUPER_USER, mysqlSuperUser);
    config.getRecord().setSimpleField(MySQLConstants.MYSQL_SUPER_PASSWORD, mysqlPassword);
    _helixAdmin.addInstance(clusterName, config);
  }

  public void createDatabase(String clusterName, String dbName, int numPartitions,
      String databaseSpec) {
    IdealState masterSlaveIdealState =
        _helixAdmin.getResourceIdealState(clusterName, MySQLConstants.MASTER_SLAVE_RESOURCE_NAME);
    int numSlices = masterSlaveIdealState.getNumPartitions();
    AutoModeISBuilder builder = new AutoModeISBuilder(dbName);
    builder.setRebalancerMode(RebalanceMode.SEMI_AUTO);
    builder.setNumPartitions(numPartitions);
    builder.setNumReplica(Integer.parseInt(masterSlaveIdealState.getReplicas()));
    builder.setStateModel("OnlineOffline");
    builder.setStateModelFactoryName("DatabaseTransitionHandlerFactory");
    for (int i = 0; i < numPartitions; i++) {
      int sliceId = i % numSlices;
      String slicePartition = MySQLConstants.MASTER_SLAVE_RESOURCE_NAME + "_" + sliceId;
      Set<String> instanceSet = masterSlaveIdealState.getInstanceSet(slicePartition);
      String dbPartitionName = dbName + "_" + i;
      builder.add(dbPartitionName);
      String[] instanceNames = new String[instanceSet.size()];
      instanceSet.toArray(instanceNames);
      builder.assignPreferenceList(dbPartitionName, instanceNames);
      Map<String, String> properties = new HashMap<String, String>();
      properties.put("sliceId", "" + sliceId);
      HelixConfigScope scope =
          new HelixConfigScopeBuilder(ConfigScopeProperty.PARTITION).forCluster(clusterName)
              .forResource(dbName).forPartition(dbPartitionName).build();
      _helixAdmin.setConfig(scope, properties);
    }
    // set the databaseSpec
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("database_spec", databaseSpec);
    properties.put("type", "DATABASE");
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(clusterName)
            .forResource(dbName).build();
    _helixAdmin.setConfig(scope, properties);

    IdealState idealState = builder.build();
    _helixAdmin.setResourceIdealState(clusterName, dbName, idealState);

  }

  public void createTable(String clusterName, String dbName, String table, String tableSpec) {
    LOG.info("Creating table:" + table + " with table_spec: " + tableSpec + " in dbName:" + dbName);
    IdealState dbIdealState = _helixAdmin.getResourceIdealState(clusterName, dbName);
    int numPartitions = dbIdealState.getNumPartitions();
    String dbTableName = dbName + "." + table;
    AutoModeISBuilder builder = new AutoModeISBuilder(dbTableName);
    builder.setRebalancerMode(RebalanceMode.SEMI_AUTO);
    builder.setNumPartitions(numPartitions);
    builder.setNumReplica(Integer.parseInt(dbIdealState.getReplicas()));
    builder.setStateModel("OnlineOffline");
    builder.setStateModelFactoryName("TableTransitionHandlerFactory");
    for (String dbPartitionName : dbIdealState.getPartitionSet()) {
      String tablePartitionName = dbPartitionName + "." + table;
      Set<String> instanceSet = dbIdealState.getInstanceSet(dbPartitionName);
      builder.add(tablePartitionName);
      String[] instanceNames = new String[instanceSet.size()];
      instanceSet.toArray(instanceNames);
      builder.assignPreferenceList(tablePartitionName, instanceNames);
    }
    IdealState idealState = builder.build();

    // before setting the idealstate, set the configuration
    Map<String, String> properties = new HashMap<String, String>();
    properties.put("table_spec", tableSpec);
    properties.put("type", "TABLE");

    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(clusterName)
            .forResource(dbTableName).build();
    _helixAdmin.setConfig(scope, properties);
    _helixAdmin.setResourceIdealState(clusterName, dbTableName, idealState);
  }

  public IdealState doInitialAssignment(String clusterName, List<String> instanceNames,
      int replicationFactor) {
    IdealState idealState = new IdealState(MySQLConstants.MASTER_SLAVE_RESOURCE_NAME);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(RebalanceMode.CUSTOMIZED);
    if (instanceNames.size() % replicationFactor != 0) {
      LOG.error(String.format(
          "Number of instances (%s) in the cluster must be a multiple of replication factor (%s)",
          instanceNames.size(), replicationFactor));
      return null;
    }
    int numSlices = instanceNames.size() / replicationFactor;
    idealState.setNumPartitions(numSlices);
    idealState.setReplicas(String.valueOf(replicationFactor));
    Collections.sort(instanceNames);
    for (int i = 0; i < numSlices; i++) {
      for (int j = 0; j < replicationFactor; j++) {
        idealState.setPartitionState(MySQLConstants.MASTER_SLAVE_RESOURCE_NAME + "_" + i,
            instanceNames.get(i * replicationFactor + j), (j == 0) ? "MASTER" : "SLAVE");
      }
    }
    LOG.info("Creating initial assignment \n" + idealState);
    _helixAdmin.setResourceIdealState(clusterName, MySQLConstants.MASTER_SLAVE_RESOURCE_NAME,
        idealState);
    return idealState;
  }

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Option zkServerOption =
        OptionBuilder.withLongOpt("zookeeperAddress").withDescription("zookeeper address").create();
    zkServerOption.setArgs(1);
    zkServerOption.setRequired(true);
    zkServerOption.setArgName("zookeeperAddress(Required)");

    OptionGroup group = new OptionGroup();
    group.setRequired(true);

    // create cluster
    Option createClusterOption =
        OptionBuilder.withLongOpt(createCluster).withDescription("Creates a new cluster").create();
    createClusterOption.setArgs(1);
    createClusterOption.setRequired(false);
    createClusterOption.setArgName("clusterName");

    // add new agent
    Option createMysqlAgent =
        OptionBuilder.withLongOpt(configureMysqlAgent)
            .withDescription("Configures a new MysqlAgent [one needed per each mysql instance]")
            .create();
    createMysqlAgent.setArgs(6);
    createMysqlAgent.setRequired(false);
    createMysqlAgent
        .setArgName("clusterName agentName[must be unique in a cluster] mysqlHost mysqlPort userName password");

    // rebalance
    // create cluster
    Option rebalanceClusterOption =
        OptionBuilder.withLongOpt(rebalanceCluster).withDescription("Creates a new cluster")
            .create();
    rebalanceClusterOption.setArgs(2);
    createClusterOption.setRequired(false);
    createClusterOption.setArgName("clusterName replicationFactor");

    // create database
    Option createDatabaseOption =
        OptionBuilder.withLongOpt(createDatabase).withDescription("Creates a new database")
            .create();
    createDatabaseOption.setArgs(4);
    createDatabaseOption.setRequired(false);
    createDatabaseOption.setArgName("clusterName databaseName numPartitions databaseSpec");

    // create table
    Option createTableOption =
        OptionBuilder.withLongOpt(createTable)
            .withDescription("Creates a new table within a database").create();
    createTableOption.setArgs(4);
    createTableOption.setRequired(false);
    createTableOption.setArgName("clusterName databaseName numPartitions databaseSpec");

    group.addOption(createClusterOption);
    group.addOption(createDatabaseOption);
    group.addOption(createTableOption);
    group.addOption(createMysqlAgent);
    group.addOption(rebalanceClusterOption);

    Options options = new Options();
    options.addOption(zkServerOption);
    options.addOptionGroup(group);
    CommandLine cliParser = new GnuParser().parse(options, args);

    String zkAddress = cliParser.getOptionValue("zookeeperAddress");
    HelixAdmin helixAdmin = null;
    if (zkAddress != null) {
      helixAdmin = new ZKHelixAdmin(zkAddress);
    }

    ClusterAdmin admin = new ClusterAdmin(helixAdmin);

    if (cliParser.hasOption(createCluster)) {
      String clusterName = cliParser.getOptionValue(createCluster);
      admin.createCluster(clusterName);
    }
    if (cliParser.hasOption(rebalanceCluster)) {
      String clusterName = cliParser.getOptionValues(rebalanceCluster)[0];
      int replicationFactor = Integer.parseInt(cliParser.getOptionValues(rebalanceCluster)[1]);
      List<String> instanceNames = helixAdmin.getInstancesInCluster(clusterName);
      admin.doInitialAssignment(clusterName, instanceNames, replicationFactor);
    }

    if (cliParser.hasOption(configureMysqlAgent)) {
      String clusterName = cliParser.getOptionValues(configureMysqlAgent)[0];
      String agentName = cliParser.getOptionValues(configureMysqlAgent)[1];
      String mysqlHost = cliParser.getOptionValues(configureMysqlAgent)[2];
      String mysqlPort = cliParser.getOptionValues(configureMysqlAgent)[3];
      String mysqlSuperUser = cliParser.getOptionValues(configureMysqlAgent)[4];
      String mysqlPassword = cliParser.getOptionValues(configureMysqlAgent)[5];
      admin.addMysqlAgent(clusterName, agentName, mysqlHost, mysqlPort, mysqlSuperUser,
          mysqlPassword);

    }
    if (cliParser.hasOption(createDatabase)) {
      String clusterName = cliParser.getOptionValues(createDatabase)[0];
      String databaseName = cliParser.getOptionValues(createDatabase)[1];
      int numPartitions = Integer.parseInt(cliParser.getOptionValues(createDatabase)[2]);
      String createDatabaseDDL = cliParser.getOptionValues(createDatabase)[3];
      admin.createDatabase(clusterName, databaseName, numPartitions, createDatabaseDDL);
    }
    if (cliParser.hasOption(createTable)) {
      String clusterName = cliParser.getOptionValues(createTable)[0];
      String databaseName = cliParser.getOptionValues(createTable)[1];
      String tableName = cliParser.getOptionValues(createTable)[2];
      String createTableDDL = cliParser.getOptionValues(createTable)[3];
      admin.createTable(clusterName, databaseName, tableName, createTableDDL);
    }

  }

}
