package org.plab;

public class TableField {
	
	
	String value;
	String fieldType;
	
	public TableField(){
		
	}
	
	public TableField setType(String type){
		this.fieldType = type;
		return this;
	}
	public TableField setValue(String value){
		this.value = value;
		return this;
	}


}
