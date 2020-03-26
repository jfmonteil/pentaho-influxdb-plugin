package org.pentaho.di.influxDB.trans.connection;

import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.core.Const;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.ui.core.PropsUI;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.gui.WindowProperty;
import org.pentaho.di.ui.core.widget.PasswordTextVar;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

/**
 * Dialog that allows you to edit the settings of a InfluxDB connection
 *
 * @author Matt
 * @see InfluxDBConnection
 */

public class InfluxDBConnectionDialog {
  private static Class<?> PKG = InfluxDBConnectionDialog.class; // for i18n purposes, needed by Translator2!!

  private InfluxDBConnection InfluxDBConnection;

  private Shell parent;
  private Shell shell;

  // Connection properties
  //
  private Text wName;
  private TextVar wHostname;
  private TextVar wPort;
  private TextVar wUsername;
  private TextVar wPassword;

  Control lastControl;

  private PropsUI props;

  private int middle;
  private int margin;

  private boolean ok;

  public InfluxDBConnectionDialog( Shell parent, InfluxDBConnection InfluxDBConnection ) {
    this.parent = parent;
    this.InfluxDBConnection = InfluxDBConnection;
    props = PropsUI.getInstance();
    ok = false;
  }

  public boolean open() {
    Display display = parent.getDisplay();
    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    shell.setImage( GUIResource.getInstance().getImageSlave() );

    middle = props.getMiddlePct();
    margin = Const.MARGIN + 2;

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;

    shell.setText( BaseMessages.getString( PKG, "InfluxDBConnectionDialog.Shell.Title" ) );
    shell.setLayout( formLayout );

    addFormWidgets();

    // Buttons
    Button wOK = new Button( shell, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );

    Button wTest = new Button( shell, SWT.PUSH );
    wTest.setText( BaseMessages.getString( PKG, "System.Button.Test" ) );

    Button wCancel = new Button( shell, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    Button[] buttons = new Button[] { wOK, wTest, wCancel };
    BaseStepDialog.positionBottomButtons( shell, buttons, margin, lastControl );

    // Add listeners
   wOK.addListener( SWT.Selection, e -> ok() );
   wTest.addListener( SWT.Selection, e -> test() );
   wCancel.addListener( SWT.Selection, e -> cancel() );


    SelectionAdapter selAdapter = new SelectionAdapter() {
      public void widgetDefaultSelected( SelectionEvent e ) {
        ok();
      }
    };
    wUsername.addSelectionListener( selAdapter );
    wPassword.addSelectionListener( selAdapter );
    wHostname.addSelectionListener( selAdapter );
    wPort.addSelectionListener( selAdapter );


    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    getData();

    BaseStepDialog.setSize( shell );

    shell.open();

    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return ok;
  }

  private void addFormWidgets() {

    // The name
    Label wlName = new Label( shell, SWT.RIGHT );
    props.setLook( wlName );
    wlName.setText( BaseMessages.getString( PKG, "InfluxDBConnectionDialog.Name.Label" ) );
    FormData fdlName = new FormData();
    fdlName.top = new FormAttachment( 0, margin );
    fdlName.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlName.right = new FormAttachment( middle, -margin );
    wlName.setLayoutData( fdlName );
    wName = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wName );
    FormData fdName = new FormData();
    fdName.top = new FormAttachment( wlName, 0, SWT.CENTER );
    fdName.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdName.right = new FormAttachment( 95, 0 );
    wName.setLayoutData( fdName );
    lastControl = wName;

    // The Hostname
    Label wlHostname = new Label( shell, SWT.RIGHT );
    props.setLook( wlHostname );
    wlHostname.setText( BaseMessages.getString( PKG, "InfluxDBConnectionDialog.Hostname.Label" ) );
    FormData fdlHostname = new FormData();
    fdlHostname.top = new FormAttachment( lastControl, margin );
    fdlHostname.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlHostname.right = new FormAttachment( middle, -margin );
    wlHostname.setLayoutData( fdlHostname );
    wHostname = new TextVar( InfluxDBConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wHostname );
    FormData fdHostname = new FormData();
    fdHostname.top = new FormAttachment( wlHostname, 0, SWT.CENTER );
    fdHostname.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdHostname.right = new FormAttachment( 95, 0 );
    wHostname.setLayoutData( fdHostname );
    lastControl = wHostname;

    // port?
    Label wlPort = new Label( shell, SWT.RIGHT );
    props.setLook( wlPort );
    wlPort.setText( BaseMessages.getString( PKG, "InfluxDBConnectionDialog.Port.Label" ) );
    FormData fdlPort = new FormData();
    fdlPort.top = new FormAttachment( lastControl, margin );
    fdlPort.left = new FormAttachment( 0, 0 ); // First one in the left top corner
    fdlPort.right = new FormAttachment( middle, -margin );
    wlPort.setLayoutData( fdlPort );
    wPort = new TextVar( InfluxDBConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPort );
    FormData fdPort = new FormData();
    fdPort.top = new FormAttachment( wlPort, 0, SWT.CENTER );
    fdPort.left = new FormAttachment( middle, 0 ); // To the right of the label
    fdPort.right = new FormAttachment( 95, 0 );
    wPort.setLayoutData( fdPort );
    lastControl = wPort;

    // Username
    Label wlUsername = new Label( shell, SWT.RIGHT );
    wlUsername.setText( BaseMessages.getString( PKG, "InfluxDBConnectionDialog.UserName.Label" ) );
    props.setLook( wlUsername );
    FormData fdlUsername = new FormData();
    fdlUsername.top = new FormAttachment( lastControl, margin );
    fdlUsername.left = new FormAttachment( 0, 0 );
    fdlUsername.right = new FormAttachment( middle, -margin );
    wlUsername.setLayoutData( fdlUsername );
    wUsername = new TextVar( InfluxDBConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wUsername );
    FormData fdUsername = new FormData();
    fdUsername.top = new FormAttachment( wlUsername, 0, SWT.CENTER );
    fdUsername.left = new FormAttachment( middle, 0 );
    fdUsername.right = new FormAttachment( 95, 0 );
    wUsername.setLayoutData( fdUsername );
    lastControl = wUsername;

    // Password
    Label wlPassword = new Label( shell, SWT.RIGHT );
    wlPassword.setText( BaseMessages.getString( PKG, "InfluxDBConnectionDialog.Password.Label" ) );
    props.setLook( wlPassword );
    FormData fdlPassword = new FormData();
    fdlPassword.top = new FormAttachment( wUsername, margin );
    fdlPassword.left = new FormAttachment( 0, 0 );
    fdlPassword.right = new FormAttachment( middle, -margin );
    wlPassword.setLayoutData( fdlPassword );
    wPassword = new PasswordTextVar( InfluxDBConnection, shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wPassword );
    FormData fdPassword = new FormData();
    fdPassword.top = new FormAttachment( wlPassword, 0, SWT.CENTER );
    fdPassword.left = new FormAttachment( middle, 0 );
    fdPassword.right = new FormAttachment( 95, 0 );
    wPassword.setLayoutData( fdPassword );
    lastControl = wPassword;

  }

  public void dispose() {
    props.setScreen( new WindowProperty( shell ) );
    shell.dispose();
  }

  public void getData() {
    wName.setText( Const.NVL( InfluxDBConnection.getName(), "" ) );
    wHostname.setText( Const.NVL( InfluxDBConnection.getHostname(), "" ) );
    wPort.setText( Const.NVL( InfluxDBConnection.getPort(), "" ) );
    wUsername.setText( Const.NVL( InfluxDBConnection.getUsername(), "" ) );
    wPassword.setText( Const.NVL( InfluxDBConnection.getPassword(), "" ) );

    wName.setFocus();
  }

  private void cancel() {
    ok = false;
    dispose();
  }

  public void ok() {
    if ( StringUtils.isEmpty( wName.getText() ) ) {
      MessageBox box = new MessageBox( shell, SWT.ICON_ERROR | SWT.OK );
      box.setText( BaseMessages.getString( PKG, "InfluxDBConnectionDialog.NoNameDialog.Title" ) );
      box.setMessage( BaseMessages.getString( PKG, "InfluxDBConnectionDialog.NoNameDialog.Message" ) );
      box.open();
      return;
    }
    getInfo( InfluxDBConnection );
    ok = true;
    dispose();
  }

  // Get dialog info in securityService
  private void getInfo( InfluxDBConnection influxDB ) {
    influxDB.setName( wName.getText() );
    influxDB.setHostname( wHostname.getText() );
    influxDB.setPort( wPort.getText() );
    influxDB.setUsername( wUsername.getText() );
    influxDB.setPassword( wPassword.getText() );
  }

  public void test() {
    InfluxDBConnection influxDB = new InfluxDBConnection( InfluxDBConnection ); // parent as variable space
    try {
      getInfo( influxDB );
      influxDB.test();
      MessageBox box = new MessageBox( shell, SWT.OK );
      box.setText( "OK" );
      String message = "Connection successful!" + Const.CR;
      message += Const.CR;
      message += "Hostname : " + influxDB.getRealHostname()+", port : "+influxDB.getRealPort()+", user : "+influxDB.getRealUsername();
      box.setMessage( message );
      box.open();
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Error connecting to InfluxDB with Hostname '" + influxDB.getRealHostname()+"', port "+influxDB.getRealPort()+", and username '"+influxDB.getRealUsername(), e );
    }
  }
}
