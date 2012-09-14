package org.plab;

import java.util.HashMap;
import java.util.Map;

public class TableObject {
	String tableName;	
	String key;			
	String keyValue;	
	Map<String, TableField> fields = new HashMap<String,TableField>();
	Map<String, Relationship> relationships = new HashMap<String, Relationship>();
	String tableLabel;
	boolean readOnly;
	String sequenceName;
	
	public TableObject(){
		
	}
	
	public TableObject setKeyValue(String value){
		this.keyValue = value;
                TableField field = fields.get(key);
                if(field != null){
                    field.setValue(value);
                }
		return this;
	}
	public TableObject setTableName(String value){
		this.tableName = value;
		return this;
	}
	public TableObject setReadOnly(boolean value){
		this.readOnly = value;
		return this;
	}
	
	public TableObject copy(){
		TableObject retVal = new TableObject();
		retVal.key = key;
		retVal.tableName = tableName;
		retVal.tableLabel = tableLabel;
		retVal.sequenceName = sequenceName;
		retVal.readOnly = readOnly;
		retVal.fields = new HashMap<String, TableField>(fields);
		retVal.relationships = new HashMap<String, Relationship>(relationships);
		return retVal;
	}
	
	
}


