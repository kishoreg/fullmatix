package org.apache.fullmatix.mysql;

import org.apache.helix.ZNRecord;
import org.apache.helix.model.IdealState;

public class TestIdealState {

  public static void main(String[] args) {
    IdealState idealState = new IdealState("id");
    idealState.setPartitionState("p0", "i001", "MASTER");
    System.out.println(idealState);
    ZNRecord record = new ZNRecord(idealState.getId());
    record.setSimpleFields(idealState.getRecord().getSimpleFields());
    record.setMapFields(idealState.getRecord().getMapFields());
    IdealState  newIdealState = new IdealState(record);
    newIdealState.setPartitionState("p0", "i001", "SLAVE");
    System.out.println(idealState);
    System.out.println(newIdealState);
    

  }
}
