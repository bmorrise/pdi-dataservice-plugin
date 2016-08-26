/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2015 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.trans.dataservice.client;

import org.pentaho.di.core.annotations.LifecyclePlugin;
import org.pentaho.di.core.lifecycle.LifeEventHandler;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.lifecycle.LifecycleListener;
import org.pentaho.di.trans.dataservice.jdbc.ThinConnection;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@LifecyclePlugin( id = "DataServiceLifecycleListener" )
public class DataServiceManagerLifecycleListener implements LifecycleListener {

  private final AtomicReference<DataServiceManagerService> dataServiceManagerService =
    new AtomicReference<DataServiceManagerService>();
  private final AtomicBoolean enabled = new AtomicBoolean( false );

  public void bind( DataServiceManagerService service ) {
    dataServiceManagerService.set( service );
    setup( service );
  }

  public void unbind( DataServiceManagerService service ) {
    dataServiceManagerService.set( null );
    setup( null );
  }

  @Override public void onStart( LifeEventHandler handler ) throws LifecycleException {
    if ( enabled.compareAndSet( false, true ) ) {
      setup( this.dataServiceManagerService.get() );
    }
  }

  @Override public void onExit( LifeEventHandler handler ) throws LifecycleException {
    if ( enabled.compareAndSet( true, false ) ) {
      setup( this.dataServiceManagerService.get() );
    }
  }

  private synchronized void setup( DataServiceManagerService managerService ) {
    if ( enabled.get() && managerService != null ) {
      ThinConnection.dataServiceManagerService = managerService;
    } else {
      ThinConnection.dataServiceManagerService = null;
    }
  }
}
