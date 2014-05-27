package org.apache.fullmatix.mysql;

import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

@StateModelInfo(initialState = "OFFLINE", states = {
    "OFFLINE", "ONLINE"
})
public class DatabaseTransitionHandler extends StateModel {

  private static final Logger LOG = Logger.getLogger(DatabaseTransitionHandler.class);

  private HelixManager _manager;
  private MySQLAdmin _mysqlAdmin;
  private String _partition;

  public DatabaseTransitionHandler(Context context, String database, String partition) {
    _manager = context.getHelixManager();
    _mysqlAdmin = context.getMysqlAdmin();
    _partition = partition;
  }

  @Transition(from = "OFFLINE", to = "ONLINE")
  public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
    LOG.info(message.getTgtName() + " transitioning from " + message.getFromState() + " to "
        + message.getToState() + " for " + _partition);
    LOG.info("Creating database: " + _partition);
    _mysqlAdmin.createDatabase(_partition);
    LOG.info(message.getTgtName() + " transitioned from " + message.getFromState() + " to "
        + message.getToState() + " for " + _partition);
  }

  @Transition(from = "ONLINE", to = "OFFLINE")
  public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
    LOG.info(_manager.getInstanceName() + " transitioning from " + message.getFromState() + " to "
        + message.getToState() + " for " + _partition);
    LOG.info("Will no longer serve this database: " + _partition);
    LOG.info(_manager.getInstanceName() + " transitioned from " + message.getFromState() + " to "
        + message.getToState() + " for " + _partition);
  }
}
