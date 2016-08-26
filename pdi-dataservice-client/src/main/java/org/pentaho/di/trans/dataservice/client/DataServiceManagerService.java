package org.pentaho.di.trans.dataservice.client;

import org.pentaho.di.trans.dataservice.jdbc.ThinServiceInformation;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by bmorrise on 8/23/16.
 */
public interface DataServiceManagerService {
  ThinServiceInformation getServiceInformation( String name ) throws SQLException;
  List<ThinServiceInformation> getServiceInformation() throws SQLException;
  List<String> getServiceNames() throws SQLException;
}
