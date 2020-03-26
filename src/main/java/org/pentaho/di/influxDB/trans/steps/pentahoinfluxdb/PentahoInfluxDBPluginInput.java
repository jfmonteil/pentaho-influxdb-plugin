/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb;

import org.apache.commons.lang.StringUtils;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.core.Const;


import org.pentaho.di.core.variables.Variables;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.sql.Timestamp;
import java.time.Instant;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.time.LocalDateTime;


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

import org.pentaho.di.influxDB.trans.connection.InfluxDBConnection;
import org.pentaho.di.influxDB.trans.connection.InfluxDBConnectionUtil;
import org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb.ReturnValue;
import org.pentaho.di.influxDB.trans.metastore.MetaStoreFactory;
import org.pentaho.di.influxDB.trans.metastore.MetaStoreUtil;

import org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb.PentahoInfluxDBPluginInputMeta;
import org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb.PentahoInfluxDBPluginInputData;

/**
 * Describe your step plugin.
 * 
 */

public class PentahoInfluxDBPluginInput extends BaseStep implements StepInterface {

  private static Class<?> PKG = PentahoInfluxDBPluginInput.class; // for i18n purposes, needed by Translator2!!
  
  private PentahoInfluxDBPluginInputMeta meta;
  private PentahoInfluxDBPluginInputData data;
  
  public PentahoInfluxDBPluginInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }
  
  /**
     * Initialize and do work where other steps need to wait for...
     *
     * @param stepMetaInterface
     *          The metadata to work with
     * @param stepDataInterface
     *          The data to initialize
     */
   @Override
   public boolean init( StepMetaInterface smi, StepDataInterface sdi ) {
        
		meta = (PentahoInfluxDBPluginInputMeta) smi;
        data = (PentahoInfluxDBPluginInputData) sdi;
        
		if ( StringUtils.isEmpty( meta.getConnectionName() ) ) {
		  log.logError( "You need to specify a InfluxDB Connection connection to use in this step" );
		  return false;
		}
	    try {
   	   	   
		   
		} catch (Exception e) {
			logError("Exception",e.getMessage(),e);
		}
		
        
		if (super.init(smi, sdi)) {
            try {
                				
				//Sheets service = new Sheets.Builder(HTTP_TRANSPORT, JSON_FACTORY, PentahoInfluxDBPluginCredentials.getCredentialsJson(scope,environmentSubstitute(meta.getJsonCredentialPath()))).setApplicationName(APPLICATION_NAME).build();
				//String range=environmentSubstitute(meta.getWorksheetId());
				//ValueRange response = service.spreadsheets().values().get(environmentSubstitute(meta.getSpreadsheetKey()),range).execute();             
			  data.metaStore = MetaStoreUtil.findMetaStore( this );
			  data.influxDBConnection = InfluxDBConnectionUtil.getConnectionFactory(data.metaStore).loadElement(environmentSubstitute(meta.getConnectionName()));
			  data.influxDBConnection.initializeVariablesFrom(this);		  
			  InfluxDB influxDB=data.influxDBConnection.connectToInfluxDB();
			  String query=environmentSubstitute(meta.getQuery());	
			 
			  influxDB.setDatabase(environmentSubstitute(meta.getDatabase()));
			  QueryResult response = influxDB.query(new Query(query));
		  //List<String> columns = result.getResults().get(0).getSeries().getColumns();
			  if(response==null) {
					logError("No response found for influxdb Database : "+environmentSubstitute( meta.getDatabase())+" for query :"+query);
			  } else 
					{				
					List<List<Object>> values = response.getResults().get(0).getSeries().get(0).getValues();
					logBasic("Reading Sheet, found: "+values.size()+" rows");
					if (values == null || values.isEmpty()) {
						logError("No response found for influxdb Database : "+environmentSubstitute( meta.getDatabase())+" for query :"+meta.getQuery());
						} else {
							data.rows=values;
						}
					}
				
            } catch (Exception e) {
                logError("Error: for influxdb Database : "+environmentSubstitute(meta.getDatabase())+" for query :"+meta.getQuery() + e.getMessage(), e);
                setErrors(1L);
                stopAll();
                return false;
            }

            return true;
        }
        return false;
    }
   

  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    
	meta = (PentahoInfluxDBPluginInputMeta) smi;
    data = (PentahoInfluxDBPluginInputData) sdi;
	
    if (first) {
		first = false;
		data.outputRowMeta = new RowMeta();
		meta.getFields(data.outputRowMeta, getStepname(), null, null, this, repository, metaStore);
		data.currentRow++;
		
	} else {
		try {
				Object[] outputRowData = readRow();
				if (outputRowData == null) {
					setOutputDone();
					return false;
				} else {
					putRow(data.outputRowMeta, outputRowData);
				}
		} catch (Exception e) {
			throw new KettleException(e.getMessage());
		} finally {
			data.currentRow++;
		}
	}

      
    return true;
  }
   private Object getRowDataValue(final ValueMetaInterface targetValueMeta, final ValueMetaInterface sourceValueMeta, final Object value, final DateFormat df,final DateTimeFormatter f) throws KettleException
    {
        if (value == null) {
            return value;
        }

        if (ValueMetaInterface.TYPE_TIMESTAMP  == targetValueMeta.getType()) {
            //Class.isAssignableFrom(Class)
			try{
			//logBasic("This is a Timestamp (type:"+sourceValueMeta.getType()+")conversion Converting :"+value.toString()+" with mask:"+sourceValueMeta.getConversionMask());

			LocalDateTime localDateTime = LocalDateTime.from(f.parse(value.toString()));
			Timestamp timestamp = Timestamp.valueOf(localDateTime);
			return targetValueMeta.convertData(sourceValueMeta, timestamp);

			
			} catch (ClassCastException exc) {
            logError("Timestamp class cast exeption");
            return targetValueMeta.convertData(sourceValueMeta, value.toString());			
			}
        }
		if (ValueMetaInterface.TYPE_STRING == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, value.toString());
        }
        
        if (ValueMetaInterface.TYPE_NUMBER == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, Double.valueOf(value.toString()));
        }
        
        if (ValueMetaInterface.TYPE_INTEGER == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, Long.valueOf(value.toString()));
        }
        
        if (ValueMetaInterface.TYPE_BIGNUMBER == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, new BigDecimal(value.toString()));
        }
        
        if (ValueMetaInterface.TYPE_BOOLEAN == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, Boolean.valueOf(value.toString()));
        }
        
        if (ValueMetaInterface.TYPE_BINARY == targetValueMeta.getType()) {
            return targetValueMeta.convertData(sourceValueMeta, value);
        }

        if (ValueMetaInterface.TYPE_DATE == targetValueMeta.getType()) {
            try {
                return targetValueMeta.convertData(sourceValueMeta, df.parse(value.toString()));
            } catch (final ParseException e) {
                throw new KettleValueException("Unable to convert data type of value");
            }
        }

        throw new KettleValueException("Unable to convert data type of value");
    }
  
   private Object[] readRow() {
        try {
            logRowlevel("Allocating :" + Integer.toString(data.outputRowMeta.size()));
			Object[] outputRowData = RowDataUtil.allocateRowData(data.outputRowMeta.size());
            int outputIndex = 0;
		    int logcur=data.currentRow;			
			logRowlevel("Reading Row: "+Integer.toString(data.currentRow)+" out of : "+Integer.toString(data.rows.size()));         
			if (data.currentRow < data.rows.size()) {
                List<Object> row= data.rows.get(data.currentRow);
                for (ValueMetaInterface column : data.outputRowMeta.getValueMetaList()) {
                Object value=null;				
				logRowlevel("Reading columns: "+Integer.toString(outputIndex)+" out of : "+Integer.toString(row.size()));
				if(outputIndex>row.size()-1){
				  logRowlevel("Beyond size"); 
				  outputRowData[outputIndex++] = null;
				}
				else {	
						if(row.get(outputIndex)!=null){
							logRowlevel("getting value" +Integer.toString(outputIndex));
							value = row.get(outputIndex);
							logRowlevel("got value "+Integer.toString(outputIndex));

						}
						if (value == null)
						{
							//||value.isEmpty()||value==""
							outputRowData[outputIndex++] = null;
							logRowlevel("null value");
						}
						else {
							DateFormat df= (column.getType() == ValueMetaInterface.TYPE_DATE)? new SimpleDateFormat(column.getConversionMask()): null;
							DateTimeFormatter f = (column.getType()==ValueMetaInterface.TYPE_TIMESTAMP)? DateTimeFormatter.ofPattern(column.getConversionMask()):null;
							outputRowData[outputIndex++] = getRowDataValue(column,column,value,df,f);
							logRowlevel("value : "+value.toString());
						}
					 }
                }
            } else {
                logBasic("Finished reading last row "+ Integer.toString(data.currentRow) +" / "+Integer.toString(data.rows.size()));
				return null;
            }
            return outputRowData;
        } catch (Exception e) {
            logError("Exception reading value :" +e.getMessage());
			return null;
        }
    }
}