package org.apache.fullmatix.mysql;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class SlaveStatus {
  enum SlaveStatusAttribute {
    Slave_IO_State,
    Master_Host,
    Master_User,
    Master_Port,
    Connect_Retry,
    Master_Log_File,
    Read_Master_Log_Pos,
    Relay_Log_File,
    Relay_Log_Pos,
    Relay_Master_Log_File,
    Slave_IO_Running,
    Slave_SQL_Running,
    Replicate_Do_DB,
    Replicate_Ignore_DB,
    Replicate_Do_Table,
    Replicate_Ignore_Table,
    Replicate_Wild_Do_Table,
    Replicate_Wild_Ignore_Table,
    Last_Errno,
    Last_Error,
    Skip_Counter,
    Exec_Master_Log_Pos,
    Relay_Log_Space,
    Until_Condition,
    Until_Log_File,
    Until_Log_Pos,
    Master_SSL_Allowed,
    Master_SSL_CA_File,
    Master_SSL_CA_Path,
    Master_SSL_Cert,
    Master_SSL_Cipher,
    Master_SSL_Key,
    Seconds_Behind_Master,
    Master_SSL_Verify_Server_Cert,
    Last_IO_Errno,
    Last_IO_Error,
    Last_SQL_Errno,
    Last_SQL_Error,
    Replicate_Ignore_Server_Ids,
    Master_Server_Id,
    Master_UUID,
    Master_Info_File,
    SQL_Delay,
    SQL_Remaining_Delay,
    Slave_SQL_Running_State,
    Master_Retry_Count,
    Master_Bind,
    Last_IO_Error_Timestamp,
    Last_SQL_Error_Timestamp,
    Master_SSL_Crl,
    Master_SSL_Crlpath,
    Retrieved_Gtid_Set,
    Executed_Gtid_Set,
    Auto_Position,
  }

  Map<String, String> valueMap = new HashMap<String, String>();

  public SlaveStatus(ResultSet rs) {
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

  String getString(SlaveStatusAttribute attribute) {
    return valueMap.get(attribute.toString().toLowerCase());
  }

  int getInt(SlaveStatusAttribute attribute) {
    String val = valueMap.get(attribute.toString().toLowerCase());
    int ret = -1;
    try {
      ret = Integer.parseInt(val);
    } catch (Exception e) {
      // ignore return default value
    }
    return ret;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (SlaveStatusAttribute attribute : SlaveStatusAttribute.values()) {
      String val = getString(attribute);
      if (val != null && val.trim().length() > 0) {
        sb.append("\n").append(attribute.toString()).append("=").append(val);
      }
    }
    return sb.toString();
  }

}
