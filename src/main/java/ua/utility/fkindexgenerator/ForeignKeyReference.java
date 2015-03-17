package ua.utility.fkindexgenerator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ForeignKeyReference implements Comparable <ForeignKeyReference> {
	private static Map <String, String> nameMap = new HashMap<String, String>();
	
	static {
	}
	
	private String schemaName;
	private String tableName;
	private String foreignKeyName;
	private String createIndexString = null;
	private String indexNameTemplate;
	private String indexName;
	
	private List<ColumnInfo> columns = new ArrayList<ColumnInfo>();

	public ForeignKeyReference(String schemaName, String tableName, String foreignKeyName, String indexNameTemplate) {
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.foreignKeyName = foreignKeyName;
        this.indexNameTemplate = indexNameTemplate;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getForeignKeyName() {
		return foreignKeyName;
	}

	public void setForeignKeyName(String foreignKeyName) {
		this.foreignKeyName = foreignKeyName;
	}

	public List<ColumnInfo> getColumns() {
		return columns;
	}

	public void setColumns(List<ColumnInfo> columns) {
		this.columns = columns;
	}
	
	public void addColumn(ColumnInfo columnInfo) {
		columns.add(columnInfo);
	}

	@Override
	public int compareTo(ForeignKeyReference o) {
		String s1 = getTableName() + "." + getForeignKeyName();
		String s2 = o.getTableName() + "." + o.getForeignKeyName();
		
		return s1.compareTo(s2);
	}
	
	public String getCreateIndexString (TableIndexInfo txinfo) {
		
		if (createIndexString == null) {
			StringBuilder buf = new StringBuilder(256);
			buf.append("create index ");

			int i = (txinfo.getMaxIndexSuffix()+1);
			
			indexName = buildIndexName(i);

			buf.append(indexName);
			
			txinfo.setMaxIndexSuffix(i);
			buf.append(" on ");
			buf.append(schemaName);
			buf.append(".");
			buf.append(getTableName());
			buf.append(" (");
			
			String comma = "";
			
			Collections.sort(columns);
			
			for (ColumnInfo c : columns) {
				buf.append(comma);
				buf.append(c.getColumnName());
				comma = ", ";
			}
			
			buf.append(")");

            
            int pos = indexName.indexOf(".");
            int len = indexName.length();
            
            if (pos > -1) {
                len = (len - (pos+1));
            }
            
            if (len > 30) {
                createIndexString = "*[" + indexName.length() + "]" + buf.toString();
            } else {
                createIndexString = buf.toString();
            }
		}
		
		return createIndexString;
	}

	private String buildIndexName(int indx) {
		StringBuilder retval = new StringBuilder(128);
		retval.append(schemaName.toUpperCase());
		retval.append(".");
        
        String nm = indexNameTemplate.replace("[table-name]", tableName).replace("{index}", "" + indx);

        retval.append(nm);
        
        return retval.toString();
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}
}
