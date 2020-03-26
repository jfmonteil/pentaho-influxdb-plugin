package org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.BoundParameterQuery.QueryBuilder;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.influxdb.impl.InfluxDBImpl;


import org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb.PentahoInfluxDBPluginInputMeta;
import org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb.ReturnValue;

import java.util.ArrayList;
import java.util.List;

@Step(
  id = "PentahoInfluxDBPluginInput",
  name = "InfluxDB Input",
  description = "Read data from InfluxDB",
  image = "PentahoInfluxDBPluginInput.svg",
  categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Input"
)
@InjectionSupported( localizationPrefix = "InfluxDB.Injection.", groups = { "PARAMETERS", "RETURNS" } )
public class PentahoInfluxDBPluginInputMeta extends BaseStepMeta implements StepMetaInterface {

  public static final String CONNECTION = "connection";
  public static final String DATABASE = "database";
  public static final String QUERY = "query";
  public static final String RETURNS = "returns";
  public static final String VARIABLES = "variables";
  public static final String RETURN = "return";
  public static final String RETURN_NAME = "return_name";
  public static final String RETURN_INFLUXDB_NAME = "return_influxDB_name";
  public static final String RETURN_TYPE = "return_type";
  public static final String RETURN_LENGTH = "return_length";
  public static final String RETURN_FORMAT = "return_format";
  

  @Injection( name = CONNECTION )
  private String connectionName;
  
  @Injection( name = DATABASE )
  private String database;

  @Injection( name = QUERY )
  private String query;
  
  @Injection( name = VARIABLES )
  private Boolean variables;

  @InjectionDeep
  private List<ReturnValue> returnValues;

  public PentahoInfluxDBPluginInputMeta() {
    super();
    returnValues = new ArrayList<>();
  }

  @Override public void setDefault() {
    query = "SELECT <field_key> FROM <measurement_name>";
	variables=false;
  }

  @Override public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta, Trans trans ) {
    return new PentahoInfluxDBPluginInput( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new PentahoInfluxDBPluginInputData();
  }

  public String getDialogClassName() {
    return PentahoInfluxDBPluginInputDialog.class.getName();
  }

  @Override public void getFields( RowMetaInterface rowMeta, String name, RowMetaInterface[] info, StepMeta nextStep, VariableSpace space,
                                   Repository repository, IMetaStore metaStore ) throws KettleStepException {

    for ( ReturnValue returnValue : returnValues ) {
      try {
        int type = ValueMetaFactory.getIdForValueMeta( returnValue.getType() );
        ValueMetaInterface valueMeta = ValueMetaFactory.createValueMeta( returnValue.getName(), type );
        valueMeta.setLength( returnValue.getLength() );
		valueMeta.setConversionMask(returnValue.getFormat());
        valueMeta.setOrigin( name );
        rowMeta.addValueMeta( valueMeta );
      } catch ( KettlePluginException e ) {
        throw new KettleStepException( "Unknown data type '" + returnValue.getType() + "' for value named '" + returnValue.getName() + "'" );
      }
    }

  }

  @Override public String getXML() {
    StringBuilder xml = new StringBuilder();
    xml.append( XMLHandler.addTagValue( CONNECTION, connectionName ) );
    xml.append( XMLHandler.addTagValue( QUERY, query ) );
	xml.append( XMLHandler.addTagValue( DATABASE, database ) );
	xml.append( "    " + XMLHandler.addTagValue( VARIABLES, variables ) );

    xml.append( XMLHandler.openTag( RETURNS ) );
    for ( ReturnValue returnValue : returnValues ) {
      xml.append( XMLHandler.openTag( RETURN ) );
      xml.append( XMLHandler.addTagValue( RETURN_NAME, returnValue.getName() ) );
      xml.append( XMLHandler.addTagValue( RETURN_INFLUXDB_NAME, returnValue.getInfluxDBName() ) );
      xml.append( XMLHandler.addTagValue( RETURN_TYPE, returnValue.getType() ) );
      xml.append( XMLHandler.addTagValue( RETURN_LENGTH, returnValue.getLength() ) );
      xml.append( XMLHandler.addTagValue( RETURN_FORMAT, returnValue.getFormat() ) );
      xml.append( XMLHandler.closeTag( RETURN ) );
    }
    xml.append( XMLHandler.closeTag( RETURNS ) );

    return xml.toString();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    connectionName = XMLHandler.getTagValue( stepnode, CONNECTION );
    query = XMLHandler.getTagValue( stepnode, QUERY );
    database = XMLHandler.getTagValue( stepnode, DATABASE );
	variables = "Y".equals( XMLHandler.getTagValue( stepnode, VARIABLES ) );

    // Parse return values
    //
    Node returnsNode = XMLHandler.getSubNode( stepnode, RETURNS );
    List<Node> returnNodes = XMLHandler.getNodes( returnsNode, RETURN );
    returnValues = new ArrayList<>();
    for ( Node returnNode : returnNodes ) {
      String name = XMLHandler.getTagValue( returnNode, RETURN_NAME );
      String influxDBName = XMLHandler.getTagValue( returnNode, RETURN_INFLUXDB_NAME );
      String type = XMLHandler.getTagValue( returnNode, RETURN_TYPE );
      int length = Const.toInt(XMLHandler.getTagValue( returnNode, RETURN_LENGTH ), -1);
      String format = XMLHandler.getTagValue( returnNode, RETURN_FORMAT );
      returnValues.add( new ReturnValue( name, influxDBName, type, length, format ) );
    }

    super.loadXML( stepnode, databases, metaStore );
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId ) throws KettleException {
    rep.saveStepAttribute( transformationId, stepId, CONNECTION, connectionName );
    rep.saveStepAttribute( transformationId, stepId, QUERY, query );
	rep.saveStepAttribute( transformationId, stepId, DATABASE, database );
	rep.saveStepAttribute( transformationId, stepId, VARIABLES, variables );

	


    for ( int i = 0; i < returnValues.size(); i++ ) {
      ReturnValue returnValue = returnValues.get( i );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_NAME, returnValue.getName() );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_INFLUXDB_NAME, returnValue.getInfluxDBName() );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_TYPE, returnValue.getType() );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_LENGTH, returnValue.getLength() );
      rep.saveStepAttribute( transformationId, stepId, i, RETURN_FORMAT, returnValue.getFormat() );
    }
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases ) throws KettleException {
    connectionName = rep.getStepAttributeString( stepId, CONNECTION );
    query = rep.getStepAttributeString( stepId, QUERY );
	database = rep.getStepAttributeString( stepId, DATABASE );
    variables = rep.getStepAttributeBoolean( stepId, VARIABLES );

    returnValues = new ArrayList<>();
    int nrReturns = rep.countNrStepAttributes( stepId, RETURN_NAME );
    for ( int i = 0; i < nrReturns; i++ ) {
      String name = rep.getStepAttributeString( stepId, i, RETURN_NAME );
      String influxDBName = rep.getStepAttributeString( stepId, i, RETURN_INFLUXDB_NAME );
      String type = rep.getStepAttributeString( stepId, i, RETURN_TYPE );
      int length = (int)rep.getStepAttributeInteger( stepId, i, RETURN_LENGTH );
      String format = rep.getStepAttributeString( stepId, i, RETURN_FORMAT );
      returnValues.add( new ReturnValue( name, influxDBName, type, length, format) );
    }

  }

  /**
   * Gets connectionName
   *
   * @return value of connectionName
   */
  public String getConnectionName() {
    return connectionName;
  }

  /**
   * @param connectionName The connectionName to set
   */
  public void setConnectionName( String connectionName ) {
    this.connectionName = connectionName;
  }
  
    /**
   * Gets database
   *
   * @return value of database
   */
  public String getDatabase() {
    return database;
  }

  /**
   * @param database The database to set
   */
  public void setDatabase( String database ) {
    this.database = database;
  }

  /**
   * Gets query
   *
   * @return value of query
   */
  public String getQuery() {
    return query;
  }

  /**
   * @param query The query to set
   */
  public void setQuery( String query ) {
    this.query = query;
  }

  /**
   * Gets returnValues
   *
   * @return value of returnValues
   */
  public List<ReturnValue> getReturnValues() {
    return returnValues;
  }

  /**
   * @param returnValues The returnValues to set
   */
  public void setReturnValues( List<ReturnValue> returnValues ) {
    this.returnValues = returnValues;
  }
  
  public boolean isVariables() {
    return variables;
  }
   
  public void setVariables( boolean variables ) {
    this.variables = variables;
  }
}


