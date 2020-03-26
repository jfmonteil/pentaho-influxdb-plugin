package org.pentaho.di.influxDB.trans.connection;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.util.PentahoDefaults;
import java.io.IOException;
import org.pentaho.di.influxDB.trans.metastore.MetaStoreFactory;
import org.pentaho.di.influxDB.trans.connection.InfluxDBConnectionDialog;

public class InfluxDBConnectionUtil {
  private static Class<?> PKG = InfluxDBConnectionUtil.class; // for i18n purposes, needed by Translator2!!

  public static MetaStoreFactory<InfluxDBConnection> getConnectionFactory( IMetaStore metaStore ) {
    return new MetaStoreFactory<InfluxDBConnection>( InfluxDBConnection.class, metaStore, PentahoDefaults.NAMESPACE );
  }

  public static InfluxDBConnection newConnection( Shell shell, VariableSpace space, MetaStoreFactory<InfluxDBConnection> factory ) {

    InfluxDBConnection connection = new InfluxDBConnection( space );
    boolean ok = false;
    while ( !ok ) {
      InfluxDBConnectionDialog dialog = new InfluxDBConnectionDialog( shell, connection );
      if ( dialog.open() ) {
        // write to metastore...
        try {
          if ( factory.loadElement( connection.getName() ) != null ) {
            MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
            box.setText( BaseMessages.getString( PKG, "InfluxDBConnectionUtil.Error.ConnectionExists.Title" ) );
            box.setMessage( BaseMessages.getString( PKG, "InfluxDBConnectionUtil.Error.ConnectionExists.Message" ) );
            int answer = box.open();
            if ( ( answer & SWT.YES ) != 0 ) {
              factory.saveElement( connection );
              ok = true;
            }
          } else {
            factory.saveElement( connection );
            ok = true;
          }
        } catch ( Exception exception ) {
          new ErrorDialog( shell,
            BaseMessages.getString( PKG, "InfluxDBConnectionUtil.Error.ErrorSavingConnection.Title" ),
            BaseMessages.getString( PKG, "InfluxDBConnectionUtil.Error.ErrorSavingConnection.Message" ),
            exception );
          return null;
        }
      } else {
        // Cancel
        return null;
      }
    }
    return connection;
  }

  public static void editConnection( Shell shell, VariableSpace space, MetaStoreFactory<InfluxDBConnection> factory, String connectionName ) {
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }
    try {
      InfluxDBConnection InfluxDBConnection = factory.loadElement( connectionName );
      InfluxDBConnection.initializeVariablesFrom( space );
      if ( InfluxDBConnection == null ) {
        newConnection( shell, space, factory );
      } else {
        InfluxDBConnectionDialog InfluxDBConnectionDialog = new InfluxDBConnectionDialog( shell, InfluxDBConnection );
        if ( InfluxDBConnectionDialog.open() ) {
          factory.saveElement( InfluxDBConnection );
        }
      }
    } catch ( Exception exception ) {
      new ErrorDialog( shell,
        BaseMessages.getString( PKG, "InfluxDBConnectionUtil.Error.ErrorEditingConnection.Title" ),
        BaseMessages.getString( PKG, "InfluxDBConnectionUtil.Error.ErrorEditingConnection.Message" ),
        exception );
    }
  }

  public static void deleteConnection( Shell shell, MetaStoreFactory<InfluxDBConnection> factory, String connectionName ) {
    if ( StringUtils.isEmpty( connectionName ) ) {
      return;
    }

    MessageBox box = new MessageBox( shell, SWT.YES | SWT.NO | SWT.ICON_ERROR );
    box.setText( BaseMessages.getString( PKG, "InfluxDBConnectionUtil.DeleteConnectionConfirmation.Title" ) );
    box.setMessage( BaseMessages.getString( PKG, "InfluxDBConnectionUtil.DeleteConnectionConfirmation.Message", connectionName ) );
    int answer = box.open();
    if ( ( answer & SWT.YES ) != 0 ) {
      try {
        factory.deleteElement( connectionName );
      } catch ( Exception exception ) {
        new ErrorDialog( shell,
          BaseMessages.getString( PKG, "InfluxDBConnectionUtil.Error.ErrorDeletingConnection.Title" ),
          BaseMessages.getString( PKG, "InfluxDBConnectionUtil.Error.ErrorDeletingConnection.Message", connectionName ),
          exception );
      }
    }
  }

}
