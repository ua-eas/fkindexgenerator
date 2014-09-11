package ua.utility.fkindexgenerator;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.Insets;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.prefs.BackingStoreException;
import java.util.prefs.Preferences;
import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.text.Document;
import org.apache.commons.lang3.StringUtils;

public class FKIndexGenerator extends JDialog implements ActionListener {
    private JTextField dbUrl;
    private JTextField dbDriver;
    private JTextField username;
    private JTextField schema;
    private JTextField indexNameTemplate;
    private JPasswordField password;
    private JTextArea sql;
    private JLabel loadInfo;
    private JButton save;
    private JButton close;
    private JButton generate;
    
	public static void main(String[] args) {
        try {
            UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
        } 
        
        catch (Exception ex) {
            ex.printStackTrace();
        }

        new FKIndexGenerator();
    }

    public FKIndexGenerator() {
        super();
        initComponents();
        pack();
        setTitle("Foreign Key Index Generator");
        setLocationRelativeTo(null);
        loadPreferences();
        setVisible(true);
    }

    @Override
    public Insets getInsets() {
        return new Insets(30, 10, 10, 10);
    }
    
    private void initComponents() {
        String[] labels = {
            "Load Information",
            "Database Driver",
            "Database URL",
            "Schema",
            "User Name",
            "Password",
            "Index Name Template"
        };
            
        loadInfo = new JLabel("");
        
        loadInfo.setFont(new Font("Arial", Font.PLAIN, 12));
        username = new JTextField(20);
        password = new JPasswordField(20);
        schema = new JTextField(30);
        dbUrl = new JTextField(30);
        dbDriver = new JTextField(30);
        indexNameTemplate = new JTextField(30);
        
        indexNameTemplate.setText("[table-name]I{index}");
        
        getContentPane().setLayout(new BorderLayout());;
        getContentPane().add(UIUtils.buildEntryPanel(labels, new JComponent[] {loadInfo, dbDriver, dbUrl, schema, username, password, indexNameTemplate}), BorderLayout.NORTH);
        getContentPane().add(new JScrollPane(sql = new JTextArea(10, 80)), BorderLayout.CENTER);
        sql.setEditable(true);
        sql.setLineWrap(false);
        JPanel p = new JPanel(new FlowLayout(FlowLayout.CENTER));
        
        p.add(generate = new JButton("Generate Indexes"));
        p.add(save = new JButton("Save Output"));
        save.setEnabled(false);
        p.add(close = new JButton("Close"));
        
        getContentPane().add(p, BorderLayout.SOUTH);
        
        save.addActionListener(this);
        close.addActionListener(this);
        generate.addActionListener(this);
    }
    
    @Override
    public void actionPerformed(ActionEvent e) {
        if (e.getSource() == save) {
            saveGeneratedIndexFile();
        } else if (e.getSource() == generate) {
            if (isReadyToConnect()) {
                sql.setText("");
                generate.setEnabled(false);
                new SwingWorker() {
                    @Override
                    public Object doInBackground() {
                        try {
                            loadForeignKeyIndexInformation();
                        }

                        catch (Exception ex) {
                            ex.printStackTrace();
                            UIUtils.showError(FKIndexGenerator.this, "Database Error", "Database error occurred while attempting to generate indexes");
                        } 

                        return null;
                    }

                    @Override
                    protected void done() {
                        save.setEnabled(sql.getText().length() > 0);
                        generate.setEnabled(true);
                    }
                }.execute();
            } else {
                UIUtils.showError(this, "Input Required", "Please complete all required connection entries (driver, url, name, password and index name template)");
            }
        } else if (e.getSource() == close) {
            handleExit();
        }
    }

    private void saveGeneratedIndexFile() {
        JFileChooser saveFile = new JFileChooser();
        if (saveFile.showSaveDialog(this) == JFileChooser.APPROVE_OPTION) {
            File f = saveFile.getSelectedFile();
            
            PrintWriter pw = null;
            
            try {
                pw = new PrintWriter(f);
                pw.println(sql.getText());
            }
            
            catch (Exception ex) {
                UIUtils.showError(this, "Save Error", "Error occurred saving output file - " + ex.toString());
            }
            
            finally {
                try {
                    if (pw != null) {
                        pw.close();
                    }
                }
                
                catch (Exception ex) {};
            }
        }
    }
    
    private boolean isReadyToConnect() {
        return (StringUtils.isNotBlank(dbUrl.getText())
            && StringUtils.isNotBlank(username.getText()) 
            && StringUtils.isNotBlank(schema.getText()) 
            && StringUtils.isNotBlank(new String(password.getPassword())) 
            && StringUtils.isNotBlank(indexNameTemplate.getText())
            && StringUtils.isNotBlank(dbDriver.getText()));
    }
            
    private void loadForeignKeyIndexInformation() throws Exception {
		Connection conn = null;
		ResultSet res = null;
		ResultSet res2 = null;
		try {
			conn = getConnection();

			Map <String, ForeignKeyReference> fkeys = new HashMap<String, ForeignKeyReference>();
			Map <String, TableIndexInfo> tindexes = new HashMap<String, TableIndexInfo>();
			
			DatabaseMetaData dmd = conn.getMetaData();
			res = dmd.getTables(null, schema.getText(), null, new String[] {"TABLE"});
			
			while (res.next()) {
				String tname = res.getString(3);
                
                updateLoadInfo("processing table " + tname);
                
				res2 = dmd.getImportedKeys(null, schema.getText(), tname);
				boolean foundfk = false;
				
				while (res2.next()) {
					foundfk = true;
					String fcname = res2.getString(8);
					int seq = res2.getInt(9);
					String fkname = res2.getString(12);
					
					ForeignKeyReference fkref = fkeys.get(fkname);
					
					if (fkref == null) {
						fkeys.put(fkname, fkref = new ForeignKeyReference(schema.getText(), tname, fkname, indexNameTemplate.getText()));
					}
					
					ColumnInfo cinfo = new ColumnInfo(fcname, seq); 
					
					cinfo.setNumeric(isNumericColumn(dmd, schema.getText(), tname, fcname));
					
					fkref.addColumn(cinfo);
				}
				
				res2.close();
				
				if (foundfk) {
					tindexes.put(tname, loadTableIndexInfo(dmd, tname));
				}
			}
			
			res.close();
			
			List <ForeignKeyReference> l = new ArrayList<ForeignKeyReference>(fkeys.values());
			
			Collections.sort(l);
			
			Iterator<ForeignKeyReference> it = l.iterator();

			while (it.hasNext()) {
				ForeignKeyReference fkref = it.next();
				if (hasIndex(tindexes.get(fkref.getTableName()).getIndexes(), fkref)) {
					it.remove();
				} else {
					String s = fkref.getCreateIndexString(tindexes.get(fkref.getTableName()));
					if (StringUtils.isNotBlank(fkref.getIndexName())) {
                        Document doc = sql.getDocument();
						sql.getDocument().insertString(doc.getLength(), s + ";\r\n", null);
					}
				}
			}
		}
		
		finally {
			closeDbObjects(null, null, res2);
			closeDbObjects(conn, null, res);
		}
	}
	
	private TableIndexInfo loadTableIndexInfo(DatabaseMetaData dmd, String tname) throws Exception {
		TableIndexInfo retval = new TableIndexInfo(tname);
		ResultSet res = null;
		
		try {
			Map <String, IndexInfo> imap = new HashMap<String, IndexInfo>();
			
			res = dmd.getIndexInfo(null, schema.getText(), tname, false, true);
			
			while (res.next()) {
				String iname = res.getString(6);
				
				if (iname != null) {
					String cname = res.getString(9);
					
					IndexInfo i = imap.get(iname);
					
					if (i == null) {
						imap.put(iname,  i = new IndexInfo(iname));
					}
					
					i.addColumn(cname);
				}
			}
			
			retval.getIndexes().addAll(imap.values());
			
			for (IndexInfo i : retval.getIndexes()) {
				String indexName = i.getIndexName();
                
                int indx = 1;
                for (int j = indexName.length()-1; j >= 0; --j) {
                    if (!Character.isDigit(indexName.charAt(j))) {
                        try {
                            indx = Integer.parseInt(indexName.substring(j+1));
                        }
                        
                        catch (NumberFormatException ex) {};
                        
                        break;
                    }
                }
            
                if (retval.getMaxIndexSuffix() < indx) {
                    retval.setMaxIndexSuffix(indx);
                }
			}
		}
		
		finally {
			closeDbObjects(null, null, res);
		}

		return retval;
	}
	
	
	private boolean hasIndex(List <IndexInfo> indexes, ForeignKeyReference fkref) throws Exception {
		boolean retval = false;

		for (IndexInfo i : indexes) {
			if (fkref.getColumns().size() == i.getIndexColumns().size()) {
				boolean foundit = true;
				for (ColumnInfo cinfo : fkref.getColumns()) {
					if (!i.getIndexColumns().contains(cinfo.getColumnName())) {
						foundit = false;
					}
				}
				
				if (foundit) {
					retval = true;
					break;
				}
			} 
		}
		
		return retval;
	}
    
    private void closeDbObjects(Connection conn, Statement stmt, ResultSet res) {
        try {
            if (res != null) {
                res.close();
            }
        } catch (SQLException ex) {
        };
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException ex) {
        };
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException ex) {
        };
    }

    private Connection getConnection() 
        throws ClassNotFoundException, SQLException, UnsupportedEncodingException {
        updateLoadInfo("connecting to database...");

        Class.forName(dbDriver.getText());
        Connection retval = DriverManager.getConnection(dbUrl.getText(), username.getText(), new String(password.getPassword()));
        retval.setReadOnly(true);
        updateLoadInfo("connected to database");
        
        return retval;
    }

	private boolean isNumericColumn(DatabaseMetaData dmd, String schema, String tname, String cname) throws Exception {
		boolean retval = false;
		
		ResultSet res = null;
		
		try {
			res = dmd.getColumns(null, schema, tname, cname);
			
			if (res.next()) {
				retval = isNumericJavaType(res.getInt(5));
				
			}
		}
		
		finally {
			closeDbObjects(null, null, res);
		}
		
		return retval;
	}
	
	private boolean isNumericJavaType(int type) {
		return ((type == java.sql.Types.BIGINT)
				|| (type == java.sql.Types.BINARY)
				|| (type == java.sql.Types.DECIMAL)
				|| (type == java.sql.Types.DOUBLE)
				|| (type == java.sql.Types.FLOAT)
				|| (type == java.sql.Types.INTEGER)
				|| (type == java.sql.Types.NUMERIC)
				|| (type == java.sql.Types.REAL)
				|| (type == java.sql.Types.SMALLINT)
				|| (type == java.sql.Types.TINYINT));
	}


    private void savePreferences() {
        try {
            Preferences proot = Preferences.userRoot();
            Preferences node = proot.node(Constants.PREFS_ROOT_NODE);

            Rectangle rect = getBounds();

            node.putInt(Constants.PREFS_MAINFRAME_LEFT, rect.x);
            node.putInt(Constants.PREFS_MAINFRAME_TOP, rect.y);
            node.putInt(Constants.PREFS_MAINFRAME_WIDTH, rect.width);
            node.putInt(Constants.PREFS_MAINFRAME_HEIGHT, rect.height);

            node.put(Constants.DATABASE_DRIVER, dbDriver.getText());
            node.put(Constants.DATABASE_URL, dbUrl.getText());
            node.put(Constants.DATABASE_USER, username.getText());
            node.put(Constants.DATABASE_SCHEMA, schema.getText());
            node.put(Constants.INDEX_NAME_TEMPLATE, indexNameTemplate.getText());

            node.flush();
        } 
        
        catch (BackingStoreException ex) {
            ex.printStackTrace();
        }
    }
    
    private void loadPreferences() {
        try {
            Preferences proot = Preferences.userRoot();
            Preferences node = proot.node(Constants.PREFS_ROOT_NODE);
            int left = node.getInt(Constants.PREFS_MAINFRAME_LEFT, Constants.MAINFRAME_DEFAULT_LEFT);
            int top = node.getInt(Constants.PREFS_MAINFRAME_TOP, Constants.MAINFRAME_DEFAULT_TOP);
            int width = node.getInt(Constants.PREFS_MAINFRAME_WIDTH, Constants.MAINFRAME_DEFAULT_WIDTH);
            int height = node.getInt(Constants.PREFS_MAINFRAME_HEIGHT, Constants.MAINFRAME_DEFAULT_HEIGHT);
            
            
            dbDriver.setText(node.get(Constants.DATABASE_DRIVER, ""));
            dbUrl.setText(node.get(Constants.DATABASE_URL, ""));
            username.setText(node.get(Constants.DATABASE_USER, ""));
            schema.setText(node.get(Constants.DATABASE_SCHEMA, ""));
            indexNameTemplate.setText(node.get(Constants.INDEX_NAME_TEMPLATE, ""));

            setBounds(left, top, width, height);
    
            node.flush();
        } 
        
        catch (BackingStoreException ex) {
            ex.printStackTrace();
        }
    }

    private void handleExit() {
        savePreferences();
        System.exit(0);
    }
    
    private void updateLoadInfo(String msg) {
        loadInfo.setText(msg);
        loadInfo.repaint(0);
    }
}
