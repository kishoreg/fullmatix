package org.apache.fullmatix.mysql;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.fullmatix.mysql.SlaveStatus.SlaveStatusAttribute;

public class MasterStatus {
  public enum MasterStatusAttribute {
    File,
    Position,
    Executed_Gtid_Set;
  }

  Map<String, String> valueMap = new HashMap<String, String>();

  public MasterStatus(ResultSet rs) {
    try {
      if (rs.next()) {
        int columnCount = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          Object object = rs.getObject(i);
          valueMap.put(rs.getMetaData().getColumnName(i).toLowerCase(),
              (object != null) ? object.toString() : null);
        }
      }
    } catch (Exception e) {
      // ignore
    }
  }

  public String getString(MasterStatusAttribute attribute) {
    return valueMap.get(attribute.toString().toLowerCase());
  }

  public int getInt(MasterStatusAttribute attribute, int defaultValue) {
    int retVal = defaultValue;
    try {
      String val = valueMap.get(attribute.toString().toLowerCase());
      if (val != null) {
        retVal = Integer.parseInt(val);
      }
    } catch (Exception e) {
      retVal = defaultValue;
    }
    return retVal;

  }
}
