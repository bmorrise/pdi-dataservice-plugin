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

package org.pentaho.di.trans.dataservice.ui.model;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.dataservice.DataServiceMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownOptimizationMeta;
import org.pentaho.di.trans.dataservice.optimization.PushDownType;
import org.pentaho.ui.xul.XulEventSourceAdapter;

import java.util.Collection;
import java.util.List;

public class DataServiceModel extends XulEventSourceAdapter {

  private List<PushDownOptimizationMeta> pushDownOptimizations = Lists.newArrayList();
  private String serviceName;
  private String serviceStep;
  private final TransMeta transMeta;

  public DataServiceModel( TransMeta transMeta ) {
    this.transMeta = transMeta;
  }

  public TransMeta getTransMeta() {
    return transMeta;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName( String serviceName ) {
    String previous = this.serviceName;
    this.serviceName = serviceName;
    firePropertyChange( "serviceName", previous, serviceName );
  }

  public String getServiceStep() {
    return serviceStep;
  }

  public void setServiceStep( String serviceStep ) {
    String previous = this.serviceStep;
    this.serviceStep = serviceStep;
    firePropertyChange( "serviceStep", previous, serviceStep );
  }

  public ImmutableList<PushDownOptimizationMeta> getPushDownOptimizations() {
    return ImmutableList.copyOf( pushDownOptimizations );
  }

  public void setPushDownOptimizations( List<PushDownOptimizationMeta> pushDownOptimizations ) {
    ImmutableList<PushDownOptimizationMeta> previous = getPushDownOptimizations();
    this.pushDownOptimizations = Lists.newArrayList( pushDownOptimizations );

    firePropertyChange( "pushDownOptimizations", previous, getPushDownOptimizations() );
  }

  public boolean add( PushDownOptimizationMeta pushDownOptimizationMeta ) {
    return addAll( ImmutableList.of( pushDownOptimizationMeta ) );
  }

  public boolean addAll( Collection<PushDownOptimizationMeta> pushDownOptimizations ) {
    ImmutableList<PushDownOptimizationMeta> previous = getPushDownOptimizations();

    if ( this.pushDownOptimizations.addAll( pushDownOptimizations ) ) {
      firePropertyChange( "pushDownOptimizations", previous, getPushDownOptimizations() );
      return true;
    }

    return false;
  }

  public boolean remove( PushDownOptimizationMeta pushDownOptimization ) {
    ImmutableList<PushDownOptimizationMeta> previous = getPushDownOptimizations();

    if ( this.pushDownOptimizations.remove( pushDownOptimization ) ) {
      firePropertyChange( "pushDownOptimizations", previous, getPushDownOptimizations() );
      return true;
    }

    return false;
  }

  public boolean removeAll( Collection<PushDownOptimizationMeta> c ) {
    ImmutableList<PushDownOptimizationMeta> previous = getPushDownOptimizations();

    if ( pushDownOptimizations.removeAll( c ) ) {
      firePropertyChange( "pushDownOptimizations", previous, getPushDownOptimizations() );
      return true;
    }

    return false;
  }

  public DataServiceMeta getDataService() {
    DataServiceMeta dataService = new DataServiceMeta( transMeta );
    dataService.setName( getServiceName() );
    dataService.setPushDownOptimizationMeta( getPushDownOptimizations() );
    dataService.setStepname( getServiceStep() );

    for ( PushDownOptimizationMeta pushDownOptimization : pushDownOptimizations ) {
      pushDownOptimization.getType().init( transMeta, dataService, pushDownOptimization );
    }

    return dataService;
  }

  public ImmutableList<PushDownOptimizationMeta> getPushDownOptimizations( final Class<? extends PushDownType> type ) {
    return FluentIterable.from( getPushDownOptimizations() )
      .filter( new Predicate<PushDownOptimizationMeta>() {
        @Override public boolean apply( PushDownOptimizationMeta input ) {
          return type.isInstance( input.getType() );
        }
      } )
      .toList();
  }
}
