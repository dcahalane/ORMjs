package org.plab;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Relationship extends TableField{
	List<TableObject> children = new ArrayList<TableObject>();
	String relatedTable;
	String foreignKey;	
	
	public Relationship(){
		super();
	}
	public Relationship setRelatedTable(String value){
		this.relatedTable = value;
		return this;
	}
	
	public Relationship setForeignKey(String key){
		this.foreignKey = key;
		return this;
	}
	public Relationship addChildObject(TableObject o){
		this.children.add(o);
		return this;
	}
	
	public TableObject getChild(String keyValue){
		for(TableObject c: children){
			if(keyValue.equals(c.keyValue)){
				return c;
			}
		}
		return null;
	}
}
