package org.apache.fullmatix.mysql;

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ConfigScope;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

@StateModelInfo(initialState = "OFFLINE", states = {
    "OFFLINE", "ONLINE"
})
public class TableTransitionHandler extends StateModel {

  private static final Logger LOG = Logger.getLogger(TableTransitionHandler.class);

  private HelixManager _manager;
  private MySQLAdmin _mysqlAdmin;

  private String _tableDDL;

  private String _databasePartitionName;

  private String _tableName;


  public TableTransitionHandler(Context context, String databaseName, String databasePartitionName, String tableName) {
    _databasePartitionName = databasePartitionName;
    _tableName = tableName;
    _manager = context.getHelixManager();
    _mysqlAdmin = context.getMysqlAdmin();
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(
            _manager.getClusterName()).forResource(databaseName + "." + tableName).build();
    _tableDDL = _manager.getConfigAccessor().get(scope, "table_spec");
  }

  @Transition(from = "OFFLINE", to = "ONLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    LOG.info(message.getTgtName() + " transitioning from " + message.getFromState() + " to "
        + message.getToState() + " for " + _databasePartitionName + "." + _tableName);
    LOG.info("Creating table: " + _tableName   + " in database: "+ _databasePartitionName + " using table_spec:" +_tableDDL);
    _mysqlAdmin.createTable(_databasePartitionName, _tableName, _tableDDL);
    LOG.info(message.getTgtName() + " transitioned from " + message.getFromState() + " to "
        + message.getToState() + " for " + _databasePartitionName + "." + _tableName);
  }

  @Transition(from = "ONLINE", to = "OFFLINE")
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
    LOG.info(_manager.getInstanceName() + " transitioning from " + message.getFromState() + " to "
        + message.getToState() + " for " +_databasePartitionName + "." + _tableName);
    LOG.info("Will no longer serve this table: " +  _databasePartitionName + "." + _tableName);
    LOG.info(_manager.getInstanceName() + " transitioned from " + message.getFromState() + " to "
        + message.getToState() + " for " +  _databasePartitionName + "." + _tableName);
  }
}
