package org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb;

import java.util.List;

import org.pentaho.di.influxDB.trans.connection.InfluxDBConnection;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.metastore.api.IMetaStore;

import java.io.InputStream;

public class PentahoInfluxDBPluginInputData extends BaseStepData implements StepDataInterface {

  public List<List<Object>> rows;
  public RowMetaInterface outputRowMeta;
  public InfluxDBConnection influxDBConnection;
  public int[] fieldIndexes;
  public String query;
  public String accessToken;
  public IMetaStore metaStore;
  public int currentRow=0;

}