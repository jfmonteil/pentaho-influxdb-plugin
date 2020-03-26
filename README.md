
# Pentaho InfluxDB Plugin

Jean-François Monteil
jfmonteil@gmail.com

InfluxDB is a Database specialized in time series, used massively for IoT projects (https://www.influxdata.com/)

The package contains 1 step :
* InfluxDB Input : Reads specified fields from a query on InfluxDB


## Installation
In *delivery rep* you will find a zip that you can unzip in your *pentaho/design-tools/data-integration/plugin* folder.
Otherwise :  ``` mvn install ```

## Input step
The step is fully *Metadata Injection* compatible

![Input Step](https://github.com/jfmonteil/Pentaho-Google-Sheet-Plugin/blob/master/screenshots/PentahoInfluxDBInputPlugin.png?raw=true)

### Step Name : Name of the step

### InfluxDB Connection
Select InfluxDB connection, create a new one *New Button*, or edit one *Edit Button*
![Input Step](https://github.com/jfmonteil/Pentaho-Google-Sheet-Plugin/blob/master/screenshots/PentahoInfluxDBInputPluginConnection.png?raw=true)

### Database 
Browse button lets you see available databases

### Query
InfluxDBQL query (on Measurement tables or continuous Query)

### Returns
*Field name : Name fo the InfluxDB field, tag or value First field should be time
*InfluxDB name : InfluxDB type : Tag or field
*Retrun type : Pentaho return type
*Length
*Format : Be careful avout timestam, "time" format : Defaul should be : yyyy-MM-dd'T'HH:mm:ss'Z'

*Get Output Fields button* lets you guess  fields and retrun types from query.

### Preview button : lets you preview X rows

