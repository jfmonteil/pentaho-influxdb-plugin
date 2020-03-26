package org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb;


import org.apache.commons.lang.StringUtils;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.ScrolledComposite;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.*;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.*;



import org.pentaho.di.influxDB.trans.connection.InfluxDBConnection;
import org.pentaho.di.influxDB.trans.connection.InfluxDBConnectionUtil;
import org.pentaho.di.influxDB.trans.steps.pentahoinfluxdb.ReturnValue;
import org.pentaho.di.influxDB.trans.metastore.MetaStoreFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.TransPreviewFactory;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.dialog.EnterNumberDialog;
import org.pentaho.di.ui.core.dialog.EnterTextDialog;
import org.pentaho.di.ui.core.dialog.ErrorDialog;
import org.pentaho.di.ui.core.dialog.PreviewRowsDialog;
import org.pentaho.di.ui.core.dialog.EnterSelectionDialog;
import org.pentaho.di.ui.core.widget.StyledTextComp;

import org.pentaho.di.ui.core.gui.GUIResource;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.ComboValuesSelectionListener;

import org.pentaho.di.core.exception.KettleException;

import org.pentaho.di.core.Props;


import org.pentaho.di.ui.spoon.Spoon;
import org.pentaho.di.ui.trans.dialog.TransPreviewProgressDialog;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

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


import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

public class PentahoInfluxDBPluginInputDialog extends BaseStepDialog implements StepDialogInterface {

  private static Class<?> PKG = PentahoInfluxDBPluginInputMeta.class; // for i18n purposes, needed by Translator2!!

  private Text wStepname;
  
  private Text wDatabase;

  private CCombo wConnection;

  private StyledTextComp wQuery;
  
  //private Label wlVariables;
  //private Button wVariables;
  private FormData fdlVariables, fdVariables;

  private TableView wReturns;

  private PentahoInfluxDBPluginInputMeta input;
  
  private Label wlPosition;
  private FormData fdlPosition;

  public PentahoInfluxDBPluginInputDialog( Shell parent, Object inputMetadata, TransMeta transMeta, String stepname ) {
    super( parent, (BaseStepMeta) inputMetadata, transMeta, stepname );
    input = (PentahoInfluxDBPluginInputMeta) inputMetadata;

    // Hack the metastore...
    //
    metaStore = Spoon.getInstance().getMetaStore();
  }

  @Override public String open() {
    Shell parent = getParent();
    Display display = parent.getDisplay();

    shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MAX | SWT.MIN );
    props.setLook( shell );
    setShellImage( shell, input );

    FormLayout shellLayout = new FormLayout();
    shell.setLayout( shellLayout );
    shell.setText( "InfluxDB Input" );
			
    ModifyListener lsMod = new ModifyListener() {
            @Override
            public void modifyText(ModifyEvent e) {
                input.setChanged();
            }
    };
    changed = input.hasChanged();

    ScrolledComposite wScrolledComposite = new ScrolledComposite( shell, SWT.V_SCROLL | SWT.H_SCROLL );
    FormLayout scFormLayout = new FormLayout();
    wScrolledComposite.setLayout( scFormLayout );
    FormData fdSComposite = new FormData();
    fdSComposite.left = new FormAttachment( 0, 0 );
    fdSComposite.right = new FormAttachment( 100, 0 );
    fdSComposite.top = new FormAttachment( 0, 0 );
    fdSComposite.bottom = new FormAttachment( 100, 0 );
    wScrolledComposite.setLayoutData( fdSComposite );

    Composite wComposite = new Composite( wScrolledComposite, SWT.NONE );
    props.setLook( wComposite );
    FormData fdComposite = new FormData();
    fdComposite.left = new FormAttachment( 0, 0 );
    fdComposite.right = new FormAttachment( 100, 0 );
    fdComposite.top = new FormAttachment( 0, 0 );
    fdComposite.bottom = new FormAttachment( 100, 0 );
    wComposite.setLayoutData( fdComposite );

    FormLayout formLayout = new FormLayout();
    formLayout.marginWidth = Const.FORM_MARGIN;
    formLayout.marginHeight = Const.FORM_MARGIN;
    wComposite.setLayout( formLayout );

    int middle = props.getMiddlePct();
    int margin = Const.MARGIN;

    // Step name line
    //
    Label wlStepname = new Label( wComposite, SWT.RIGHT );
    wlStepname.setText( "Step name" );
    props.setLook( wlStepname );
    fdlStepname = new FormData();
    fdlStepname.left = new FormAttachment( 0, 0 );
    fdlStepname.right = new FormAttachment( middle, -margin );
    fdlStepname.top = new FormAttachment( 0, margin );
    wlStepname.setLayoutData( fdlStepname );
    wStepname = new Text( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wStepname );
    wStepname.addModifyListener( lsMod );
    fdStepname = new FormData();
    fdStepname.left = new FormAttachment( middle, 0 );
    fdStepname.top = new FormAttachment( wlStepname, 0, SWT.CENTER );
    fdStepname.right = new FormAttachment( 100, 0 );
    wStepname.setLayoutData( fdStepname );
    Control lastControl = wStepname;


    Label wlConnection = new Label( wComposite, SWT.RIGHT );
    wlConnection.setText( "InfluxDB Connection" );
    props.setLook( wlConnection );
    FormData fdlConnection = new FormData();
    fdlConnection.left = new FormAttachment( 0, 0 );
    fdlConnection.right = new FormAttachment( middle, -margin );
    fdlConnection.top = new FormAttachment( lastControl, 2 * margin );
    wlConnection.setLayoutData( fdlConnection );

    Button wEditConnection = new Button( wComposite, SWT.PUSH | SWT.BORDER );
    wEditConnection.setText( BaseMessages.getString( PKG, "System.Button.Edit" ) );
    FormData fdEditConnection = new FormData();
    fdEditConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdEditConnection.right = new FormAttachment( 100, 0 );
    wEditConnection.setLayoutData( fdEditConnection );

    Button wNewConnection = new Button( wComposite, SWT.PUSH | SWT.BORDER );
    wNewConnection.setText( BaseMessages.getString( PKG, "System.Button.New" ) );
    FormData fdNewConnection = new FormData();
    fdNewConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    fdNewConnection.right = new FormAttachment( wEditConnection, -margin );
    wNewConnection.setLayoutData( fdNewConnection );

    wConnection = new CCombo( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wConnection );
    wConnection.addModifyListener( lsMod );
    FormData fdConnection = new FormData();
    fdConnection.left = new FormAttachment( middle, 0 );
    fdConnection.right = new FormAttachment( wNewConnection, -margin );
    fdConnection.top = new FormAttachment( wlConnection, 0, SWT.CENTER );
    wConnection.setLayoutData( fdConnection );
    lastControl = wConnection;
	
	 // Database line
    //Label
    Label wlDatabase = new Label( wComposite, SWT.RIGHT );
    wlDatabase.setText( "Database" );
    props.setLook( wlDatabase );
    FormData fdlDatabase = new FormData();
    fdlDatabase.left = new FormAttachment( 0, 0 );
    fdlDatabase.right = new FormAttachment( middle, -margin );
    fdlDatabase.top = new FormAttachment( lastControl, margin );
    wlDatabase.setLayoutData( fdlDatabase );
    //  Button
	Button databaseButton = new Button(wComposite, SWT.PUSH | SWT.CENTER);
	databaseButton.setText("Browse");
	props.setLook(databaseButton);
	FormData databaseButtonData = new FormData();
	databaseButtonData.top = new FormAttachment(lastControl,margin);
	databaseButtonData.right = new FormAttachment(100, 0);
	databaseButton.setLayoutData(databaseButtonData);
	//textfield
    wDatabase = new Text( wComposite, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
    props.setLook( wDatabase );
    wDatabase.addModifyListener( lsMod );
    FormData fdDatabase = new FormData();
    fdDatabase.left = new FormAttachment( middle, 0 );
    fdDatabase.top = new FormAttachment( lastControl, margin );
    fdDatabase.right = new FormAttachment( databaseButton, -margin );
    wDatabase.setLayoutData( fdDatabase );
    lastControl = databaseButton;
	
	 	//query
    Label wlQuery = new Label( wComposite, SWT.LEFT );
    wlQuery.setText( "Query:" );
    props.setLook( wlQuery );
    FormData fdlQuery = new FormData();
    fdlQuery.left = new FormAttachment( 0, 0 );
    fdlQuery.right = new FormAttachment( middle, -margin );
    fdlQuery.top = new FormAttachment( lastControl, margin );
    wlQuery.setLayoutData( fdlQuery );
	lastControl=wlQuery;
    wQuery = new StyledTextComp( transMeta, wComposite, SWT.MULTI | SWT.LEFT | SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL, "" );
    wQuery.setFont( GUIResource.getInstance().getFontFixed() );
    props.setLook( wQuery,Props.WIDGET_STYLE_FIXED);     
	wQuery.addModifyListener( lsMod );
    FormData fdQuery = new FormData();
    fdQuery.left = new FormAttachment( 0, 0 );
    fdQuery.right = new FormAttachment( databaseButton, 0 );
    fdQuery.top = new FormAttachment( lastControl, margin );
    fdQuery.bottom = new FormAttachment( lastControl, 200 +margin);
    wQuery.setLayoutData( fdQuery );
    lastControl = wQuery;
	

	//Cursor position in query window
	wlPosition = new Label( wComposite, SWT.NONE );
    props.setLook( wlPosition );
    fdlPosition = new FormData();
    fdlPosition.left = new FormAttachment( 0, 0 );
    fdlPosition.right = new FormAttachment( 100, 0 );
    fdlPosition.top = new FormAttachment( lastControl, margin );
    wlPosition.setLayoutData( fdlPosition );
	lastControl = wlPosition;
	
		   // Replace variables in SQL?
   /* wlVariables = new Label( wComposite, SWT.RIGHT );
    wlVariables.setText("Replace variables in SQL");
    props.setLook( wlVariables );
    fdlVariables = new FormData();
    fdlVariables.left = new FormAttachment( 0, 0 );
    fdlVariables.right = new FormAttachment( middle, -margin );
    fdlVariables.top = new FormAttachment( lastControl, margin );
    wlVariables.setLayoutData( fdlVariables );
    wVariables = new Button( wComposite, SWT.CHECK );
    props.setLook( wVariables );
    fdVariables = new FormData();
    fdVariables.left = new FormAttachment( middle, 0 );
    fdVariables.right = new FormAttachment( 100, 0 );
    fdVariables.top = new FormAttachment( lastControl,margin);
    wVariables.setLayoutData( fdVariables );*/




    Label wlReturns = new Label( wComposite, SWT.LEFT );
    wlReturns.setText( "Returns" );
    props.setLook( wlReturns );
    FormData fdlReturns = new FormData();
    fdlReturns.left = new FormAttachment( 0, 0 );
    fdlReturns.right = new FormAttachment( middle, -margin );
    fdlReturns.top = new FormAttachment( lastControl, margin );
    wlReturns.setLayoutData( fdlReturns );

    Button wbGetReturnFields = new Button( wComposite, SWT.PUSH );
    wbGetReturnFields.setText( "Get Output Fields" );
    FormData fdbGetReturnFields = new FormData();
    fdbGetReturnFields.right = new FormAttachment( 100, 0 );
    fdbGetReturnFields.top = new FormAttachment( wlReturns, margin );
    wbGetReturnFields.setLayoutData( fdbGetReturnFields );
   // wbGetReturnFields.addSelectionListener( lsReturn );

        // Table: return field name and type
    //
    ColumnInfo[] returnColumns =
      new ColumnInfo[] {
        new ColumnInfo( "Field name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "InfluxDB name", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Return type", ColumnInfo.COLUMN_TYPE_CCOMBO, ValueMetaFactory.getAllValueMetaNames(), false ),
        new ColumnInfo( "Length", ColumnInfo.COLUMN_TYPE_TEXT, false ),
        new ColumnInfo( "Format", ColumnInfo.COLUMN_TYPE_TEXT, false ),
    };
	
	wReturns = new TableView( transMeta, wComposite, SWT.FULL_SELECTION | SWT.MULTI, returnColumns, input.getReturnValues().size(), lsMod, props );
    props.setLook( wReturns );
    wReturns.addModifyListener( lsMod );
    FormData fdReturns = new FormData();
    fdReturns.left = new FormAttachment( 0, 0 );
    fdReturns.right = new FormAttachment( wbGetReturnFields, 0 );
    fdReturns.top = new FormAttachment( wlReturns, margin );
    fdReturns.bottom = new FormAttachment( wlReturns, 300 + margin );
    wReturns.setLayoutData( fdReturns );
    lastControl = wReturns;
	
		
   
    
	/*wVariables.addSelectionListener( new SelectionAdapter() {
      public void widgetSelected( SelectionEvent arg0 ) {
        input.setChanged();
        setSQLToolTip();
      }
    } );*/

	
    wComposite.pack();
    Rectangle bounds = wComposite.getBounds();

    wScrolledComposite.setContent( wComposite );
    wScrolledComposite.setExpandHorizontal( true );
    wScrolledComposite.setExpandVertical( true );
    wScrolledComposite.setMinWidth( bounds.width );
    wScrolledComposite.setMinHeight( bounds.height );

    // Some buttons
    wOK = new Button( wComposite, SWT.PUSH );
    wOK.setText( BaseMessages.getString( PKG, "System.Button.OK" ) );
    wPreview = new Button( wComposite, SWT.PUSH );
    wPreview.setText( BaseMessages.getString( PKG, "System.Button.Preview" ) );
    wCancel = new Button( wComposite, SWT.PUSH );
    wCancel.setText( BaseMessages.getString( PKG, "System.Button.Cancel" ) );

    // Position the buttons at the bottom of the dialog.
    //
    setButtonPositions( new Button[] { wOK, wPreview, wCancel }, margin, null );

	
	// Add listeners
    //
    wCancel.addListener( SWT.Selection, e -> cancel() );
    wOK.addListener( SWT.Selection, e -> ok() );
    wPreview.addListener( SWT.Selection, e -> preview() );

    wConnection.addModifyListener( lsMod );
    wStepname.addModifyListener( lsMod );
    wDatabase.addModifyListener( lsMod );
	
	   // Text Higlighting
    wQuery.addLineStyleListener( new SQLValuesHighlight() );
	
		//Highlight
	wQuery.addModifyListener( new ModifyListener() {
      public void modifyText( ModifyEvent arg0 ) {
        setSQLToolTip();
        setPosition();
      }
    } );

    wQuery.addKeyListener( new KeyAdapter() {
      public void keyPressed( KeyEvent e ) {
        setPosition();
      }

      public void keyReleased( KeyEvent e ) {
        setPosition();
      }
    } );
    wQuery.addFocusListener( new FocusAdapter() {
      public void focusGained( FocusEvent e ) {
        setPosition();
      }

      public void focusLost( FocusEvent e ) {
        setPosition();
      }
    } );
    wQuery.addMouseListener( new MouseAdapter() {
      public void mouseDoubleClick( MouseEvent e ) {
        setPosition();
      }

      public void mouseDown( MouseEvent e ) {
        setPosition();
      }

      public void mouseUp( MouseEvent e ) {
        setPosition();
      }
    } );
	
	//Populate return fields
    wbGetReturnFields.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                try{
				getReturnValues();
				} catch (Exception ex) {
					new ErrorDialog( shell, "Error", "Error retrieving retunr fields" , ex );

				}					
            }
        });
		
	// Chose Database Button
	databaseButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {    				
   				try {
						InfluxDB influxDB;
						MetaStoreFactory<InfluxDBConnection> factory = InfluxDBConnectionUtil.getConnectionFactory(metaStore);
						InfluxDBConnection influxDBConnection = factory.loadElement(wConnection.getText());
						influxDBConnection.initializeVariablesFrom(transMeta);					
						String query="SHOW DATABASES";						
						influxDB=influxDBConnection.connectToInfluxDB();
						
						QueryResult result = influxDB.query(new Query(query));
						if(result!=null)
						{
							List<List<Object>> databaseNames = result.getResults().get(0).getSeries().get(0).getValues();
									
							//List<String> databases = Lists.newArrayList();
							int selectedDatabase= -1;
							int i=0;
							if (databaseNames != null) 
							{
								String[] databasesList=new String[databaseNames.size()];

								for (List<Object> database : databaseNames) {
									databasesList[i]=(database.get(0).toString());
									if(wDatabase!=null && !wDatabase.getText().isEmpty() && databasesList[i].equals(wDatabase.getText())){
										selectedDatabase = i;	
									}
									i++;	
								}
								
								EnterSelectionDialog esd = new EnterSelectionDialog(shell, databasesList, "Databases", "Select a Database.");
								if (selectedDatabase > -1) {
									esd.setSelectedNrs(new int[]{selectedDatabase});
								}
								String s=esd.open();
								if(s!=null)
								{
									if (esd.getSelectionIndeces().length > 0) {
										selectedDatabase = esd.getSelectionIndeces()[0];
										String db = databasesList[selectedDatabase];
										if(db!=null){
											wDatabase.setText(db);
										}										
									} 
									else {
										wDatabase.setText("");
									}
								}
								
							}
						}
						influxDB.close();
					} catch(Exception ex) {
					        new ErrorDialog( shell, "Error", "Error retrieving databases",ex );
					}
			}
	});
    wNewConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        newConnection();
      }
    } );
    wEditConnection.addSelectionListener( new SelectionAdapter() {
      @Override public void widgetSelected( SelectionEvent selectionEvent ) {
        editConnection();
      }
    } );

    // Detect X or ALT-F4 or something that kills this window...
    shell.addShellListener( new ShellAdapter() {
      public void shellClosed( ShellEvent e ) {
        cancel();
      }
    } );

    // Set the shell size, based upon previous time...
    setSize();

    getData();
    input.setChanged( changed );

    shell.open();
    while ( !shell.isDisposed() ) {
      if ( !display.readAndDispatch() ) {
        display.sleep();
      }
    }
    return stepname;

  }
  
  public void setPosition() {

    String scr = wQuery.getText();
    int linenr = wQuery.getLineAtOffset( wQuery.getCaretOffset() ) + 1;
    int posnr = wQuery.getCaretOffset();

    // Go back from position to last CR: how many positions?
    int colnr = 0;
    while ( posnr > 0 && scr.charAt( posnr - 1 ) != '\n' && scr.charAt( posnr - 1 ) != '\r' ) {
      posnr--;
      colnr++;
    }
    wlPosition.setText("Position :"+ linenr +" " + colnr);

  }

  protected void setSQLToolTip() {
  /*  if ( wVariables.getSelection() ) {
      wQuery.setToolTipText( transMeta.environmentSubstitute( wQuery.getText() ) );
    }*/
  }

  private void cancel() {
    stepname = null;
    input.setChanged( changed );
    dispose();
  }

  public void getData() {

    wStepname.setText( Const.NVL( stepname, "" ) );
    wConnection.setText( Const.NVL( input.getConnectionName(), "" ) );
    wDatabase.setText(Const.NVL(input.getDatabase(),""));
	//wVariables.setSelection( input.isVariables() );
	wQuery.setText( Const.NVL( input.getQuery(), "SELECT <field_key> FROM <measurement_name>" ) );
    // List of connections...
    //
    try {
      List<String> elementNames = InfluxDBConnectionUtil.getConnectionFactory( metaStore ).getElementNames();
      Collections.sort( elementNames );
      wConnection.setItems( elementNames.toArray( new String[ 0 ] ) );
    } catch ( Exception e ) {
      new ErrorDialog( shell, "Error", "Unable to list InfluxDB connections", e );
    }


    for ( int i = 0; i < input.getReturnValues().size(); i++ ) {
      ReturnValue returnValue = input.getReturnValues().get( i );
      TableItem item = wReturns.table.getItem( i );
      item.setText( 1, Const.NVL( returnValue.getName(), "" ) );
      item.setText( 2, Const.NVL( returnValue.getInfluxDBName(), "" ) );
      item.setText( 3, Const.NVL( returnValue.getType(), "" ) );
      item.setText( 4, returnValue.getLength() < 0 ? "" : Integer.toString( returnValue.getLength() ) );
      item.setText( 5, Const.NVL( returnValue.getFormat(), "" ) );
    }
    wReturns.removeEmptyRows();
    wReturns.setRowNums();
    wReturns.optWidth( true );

  }

  private void ok() {
    if ( StringUtils.isEmpty( wStepname.getText() ) ) {
      return;
    }
    stepname = wStepname.getText(); // return value
    getInfo( input );
    dispose();
  }

  private void getInfo( PentahoInfluxDBPluginInputMeta meta ) {
    meta.setConnectionName( wConnection.getText() );
    meta.setQuery( wQuery.getText() );
	meta.setDatabase(wDatabase.getText());
	/*meta.setVariables(wVariables.getSelection());*/

    List<ReturnValue> returnValues = new ArrayList<>();
    for ( int i = 0; i < wReturns.nrNonEmpty(); i++ ) {
      TableItem item = wReturns.getNonEmpty( i );
      String name = item.getText( 1 );
      String influxDBName = item.getText( 2 );
      String type = item.getText( 3 );
      int length = Const.toInt( item.getText( 4 ), -1 );
      String format = item.getText( 5 );
      returnValues.add( new ReturnValue( name, influxDBName, type, length, format ) );
    }
    meta.setReturnValues( returnValues );
  }

  protected void newConnection() {
    InfluxDBConnection connection = InfluxDBConnectionUtil.newConnection( shell, transMeta, InfluxDBConnectionUtil.getConnectionFactory( metaStore ) );
    if ( connection != null ) {
      wConnection.setText( connection.getName() );
    }
  }

  protected void editConnection() {
    InfluxDBConnectionUtil.editConnection( shell, transMeta, InfluxDBConnectionUtil.getConnectionFactory( metaStore ), wConnection.getText() );
  }

  private synchronized void preview() {
    PentahoInfluxDBPluginInputMeta oneMeta = new PentahoInfluxDBPluginInputMeta();
    this.getInfo( oneMeta );
    TransMeta previewMeta = TransPreviewFactory.generatePreviewTransformation( this.transMeta, oneMeta, this.wStepname.getText() );
    this.transMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    previewMeta.getVariable( "Internal.Transformation.Filename.Directory" );
    EnterNumberDialog
      numberDialog = new EnterNumberDialog( this.shell, this.props.getDefaultPreviewSize(),
      BaseMessages.getString( PKG, "QueryDialog.PreviewSize.DialogTitle" ),
      BaseMessages.getString( PKG, "QueryDialog.PreviewSize.DialogMessage" )
    );
    int previewSize = numberDialog.open();
    if ( previewSize > 0 ) {
      TransPreviewProgressDialog progressDialog = new TransPreviewProgressDialog( this.shell, previewMeta, new String[] { this.wStepname.getText() }, new int[] { previewSize } );
      progressDialog.open();
      Trans trans = progressDialog.getTrans();
      String loggingText = progressDialog.getLoggingText();
      if ( !progressDialog.isCancelled() && trans.getResult() != null && trans.getResult().getNrErrors() > 0L ) {
        EnterTextDialog etd = new EnterTextDialog( this.shell,
          BaseMessages.getString( PKG, "System.Dialog.PreviewError.Title", new String[ 0 ] ),
          BaseMessages.getString( PKG, "System.Dialog.PreviewError.Message", new String[ 0 ] ), loggingText, true );
        etd.setReadOnly();
        etd.open();
      }

      PreviewRowsDialog prd = new PreviewRowsDialog( this.shell, this.transMeta, 0, this.wStepname.getText(), progressDialog.getPreviewRowsMeta( this.wStepname.getText() ),
        progressDialog.getPreviewRows( this.wStepname.getText() ), loggingText );
      prd.open();
    }
  }
  
  private String converType(String initType){
	  
      String destType="String";
	  if(initType==null || initType.isEmpty())
		  return destType;
	  switch(initType.toLowerCase())
	  {
		case "float" : destType ="Number"; break;
		case "boolean" : destType ="Boolean"; break;
		case "integer" : destType= "Integer"; break;
        case "string" : destType = "String"; break;
	  }	
      return destType;	  
  }

  
  private void getReturnValues() throws KettleException {

    try {

		  MetaStoreFactory<InfluxDBConnection> factory = InfluxDBConnectionUtil.getConnectionFactory( metaStore );
		  InfluxDBConnection influxDBConnection = factory.loadElement(this.transMeta.environmentSubstitute(wConnection.getText()));
		  influxDBConnection.initializeVariablesFrom( this.transMeta );
		  InfluxDB influxDB;  
		  String query=this.transMeta.environmentSubstitute(wQuery.getText());
          		  
		  String database=this.transMeta.environmentSubstitute(wDatabase.getText());
		  influxDB=influxDBConnection.connectToInfluxDB();
		  //influxDB.setDatabase(database);
		  
		  //Working on query to extract tags, columns
		  if(query!=null && !query.isEmpty())
		  {
			  TableItem itemTime = new TableItem(wReturns.table, SWT.NONE);
              itemTime.setText(1, "time");
			  itemTime.setText(2, "Time");
			  itemTime.setText(3, "Timestamp");
			  itemTime.setText(4, "");
			  itemTime.setText(5, "yyyy-MM-dd'T'HH:mm:ss'Z'");			  
			 
			  int startPos=query.toLowerCase().indexOf("from");
			  String showTagsQuery="SHOW TAG KEYS ON "+database+" "+query.substring(startPos);
			  
			  QueryResult result = influxDB.query(new Query(showTagsQuery));
			  List<List<Object>> columns = result.getResults().get(0).getSeries().get(0).getValues();

			  if (columns != null) 
				{
					for (List<Object> column : columns) {
							if(column!=null && column.get(0)!=null && !column.get(0).toString().isEmpty())
							{
								TableItem item = new TableItem(wReturns.table, SWT.NONE);
								item.setText(1, column.get(0).toString());
								item.setText(2, "Tag");
								item.setText(3, "String");
								item.setText(4, "");
								item.setText(5, "");
							}
						}
						
				}
			 String showFieldsQuery="SHOW FIELD KEYS ON "+database+" "+query.substring(startPos);
			 result = influxDB.query(new Query(showFieldsQuery));
			 List<List<Object>> values = result.getResults().get(0).getSeries().get(0).getValues();
			 
			 if (values != null) 
				{
					for (List<Object> fields : values) {
						if(columns!=null && !columns.isEmpty()){
							TableItem itemF = new TableItem(wReturns.table, SWT.NONE);
							itemF.setText(1, fields.get(0).toString());
							itemF.setText(2, "Field");
							itemF.setText(3, converType(fields.get(1).toString()));
							itemF.setText(4, "");
							itemF.setText(5, "");
							}
						}
						
				}
		  }
		  /*Map<String, Object> tags = new HashMap<String, Object>(); 
		  if(result.getResults().get(0).getSeries().get(0).getTags()!=null) {
			tags.putAll(result.getResults().get(0).getSeries().get(0).getTags());
          }

		  if (tags != null) 
			{
				for (Map.Entry<String, Object> entry : tags.entrySet()) {
					//System.out.println("Item : " + entry.getKey() + " Count : " + entry.getValue());
					TableItem item = new TableItem(wReturns.table, SWT.NONE);
					item.setText(1, entry.getKey());
					item.setText(2, "Tag");
					item.setText(3, "String");
				}											
			}
		 */
		 influxDB.close();
		
		
		} catch(Exception e) {
		  throw new KettleException( "Error connecting to InfluxDB connection to get databases", e );
		}
	  

    
        
     }
	 



}
