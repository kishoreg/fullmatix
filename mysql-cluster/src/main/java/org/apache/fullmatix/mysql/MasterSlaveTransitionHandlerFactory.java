package org.apache.fullmatix.mysql;

import org.apache.helix.participant.statemachine.StateModelFactory;

public class MasterSlaveTransitionHandlerFactory extends StateModelFactory<MasterSlaveTransitionHandler> {

  private Context _context;

  public MasterSlaveTransitionHandlerFactory(Context context) {
    _context = context;
  }

  @Override
  public MasterSlaveTransitionHandler createNewStateModel(String partition) {
    return new MasterSlaveTransitionHandler(_context, partition.split("_")[0],
        partition.toString());
  }

}
