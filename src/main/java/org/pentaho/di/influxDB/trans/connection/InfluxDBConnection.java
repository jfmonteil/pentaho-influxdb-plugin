package org.pentaho.di.influxDB.trans.connection;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.LogLevel;
import org.influxdb.InfluxDB.ResponseFormat;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.BoundParameterQuery.QueryBuilder;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.influxdb.impl.InfluxDBImpl;

import okhttp3.OkHttpClient;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.metastore.persist.MetaStoreAttribute;
import org.pentaho.metastore.persist.MetaStoreElementType;

import java.io.IOException;
import java.util.Objects;

import org.pentaho.di.influxDB.trans.metastore.MetaStoreFactory;
import org.pentaho.di.influxDB.trans.connection.InfluxDBConnectionDialog;

@MetaStoreElementType(
  name = "InfluxDB Connection",
  description = "This element describes how you can connect to InfluxDB"
)
public class InfluxDBConnection extends Variables {

  private String name;

  @MetaStoreAttribute
  private String hostname;

  @MetaStoreAttribute
  private String port;

  @MetaStoreAttribute
  private String username;

  @MetaStoreAttribute(password = true)
  private String password;
  
  private ResponseFormat responseFormat;
  
  private String apiUrlToUse;

  public InfluxDBConnection() {
    super();
  }

  public InfluxDBConnection( VariableSpace parent ) {
    super.initializeVariablesFrom( parent );
  }

  public InfluxDBConnection( VariableSpace parent, InfluxDBConnection source ) {
    super.initializeVariablesFrom( parent );
    this.name = source.name;
    this.hostname = source.hostname;
    this.port = source.port;
    this.username = source.username;
    this.password = source.password;
	this.responseFormat=ResponseFormat.JSON;
  }

  public InfluxDBConnection( String name, String hostname, String port, String username, String password) {
    this.name = name;
    this.hostname = hostname;
    this.port = port;
    this.username = username;
    this.password = password;
	this.responseFormat=ResponseFormat.JSON;
  }

  public Boolean test() throws KettleException {
    String apiUrlToUse="";
    InfluxDB influxDB;
	OkHttpClient.Builder clientToUse;
    clientToUse = new OkHttpClient.Builder();
	try {
	  this.apiUrlToUse = "http://" + this.getRealHostname() + ":" + this.getRealPort();
      influxDB=InfluxDBFactory.connect(this.apiUrlToUse,this.getRealUsername(),Encr.decryptPasswordOptionallyEncrypted(this.getRealPassword()),clientToUse,this.getRealResponseFormat());
	  Pong result = influxDB.ping();
	  if (result.isGood()) {
          return true;
      }
	  else return false;
	
	} catch(Exception e) {
      throw new KettleException( "Error connecting to InfluxDB connection", e );
    }
  }

  /*public ServiceArgs getServiceArgs() {
    ServiceArgs args = new ServiceArgs();
    args.setUsername( getRealUsername() );
    args.setPassword( Encr.decryptPasswordOptionallyEncrypted( getRealPassword() ) );
    args.setHost( getRealHostname() );
    args.setPort( Const.toInt( getRealPort(), 8089 ) );
	args.setResponseFormat(getRealResponseFormat());
    return args;
  }*/

  @Override public boolean equals( Object o ) {
    if ( this == o ) {
      return true;
    }
    if ( o == null || getClass() != o.getClass() ) {
      return false;
    }
    InfluxDBConnection that = (InfluxDBConnection) o;
    return name.equals( that.name );
  }

  @Override public int hashCode() {
    return name == null ? super.hashCode() : name.hashCode();
  }

  @Override public String toString() {
    return name == null ? super.toString() : name;
  }

  public String getRealHostname() {
    return environmentSubstitute( hostname );
  }

  public String getRealPort() {
    return environmentSubstitute( port );
  }

  public String getRealUsername() {
    return environmentSubstitute( username );
  }
  
  public ResponseFormat getRealResponseFormat() {
    return ResponseFormat.JSON;
  }

  public String getApiUrlToUse()
  {
	if(!getRealHostname().isEmpty() && !getRealPort().isEmpty())
	{
		String apiUrlToUse = "http://" + getRealHostname() + ":" + getRealPort();
		return apiUrlToUse;
	}
	else return "";

  }

  public String getRealPassword() {
    return Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( password ) );
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  public void setName( String name ) {
    this.name = name;
  }

  /**
   * Gets hostname
   *
   * @return value of hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname The hostname to set
   */
  public void setHostname( String hostname ) {
    this.hostname = hostname;
  }

  /**
   * Gets port
   *
   * @return value of port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port The port to set
   */
  public void setPort( String port ) {
    this.port = port;
  }

  /**
   * Gets username
   *
   * @return value of username
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username The username to set
   */
  public void setUsername( String username ) {
    this.username = username;
  }

  /**
   * Gets password
   *
   * @return value of password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password The password to set
   */
  public void setPassword( String password ) {
    this.password = password;
  }
  
  
  //connection to influxdb.
  /*public static InfluxDB connectToInfluxDB() throws InterruptedException, IOException {
    return connectToInfluxDB(null, null, ResponseFormat.JSON);
  }*/

  /*public static InfluxDB connectToInfluxDB(ResponseFormat responseFormat) throws InterruptedException, IOException {
    return connectToInfluxDB(null, null, responseFormat);
  }*/
  /*public static InfluxDB connectToInfluxDB(String apiUrl) throws InterruptedException, IOException {
    return connectToInfluxDB(new OkHttpClient.Builder(), apiUrl, ResponseFormat.JSON);
  }*/
  
  public InfluxDB connectToInfluxDB() throws InterruptedException, IOException {
    
	OkHttpClient.Builder clientToUse;
    clientToUse = new OkHttpClient.Builder();
    InfluxDB influxDB = InfluxDBFactory.connect("http://" + environmentSubstitute( this.hostname ) + ":" + environmentSubstitute( this.port ), environmentSubstitute( this.username ), Encr.decryptPasswordOptionallyEncrypted( environmentSubstitute( this.password )), clientToUse, ResponseFormat.JSON);
    influxDB.setLogLevel(InfluxDB.LogLevel.NONE);
   
    return influxDB;
  }
  
  
}
