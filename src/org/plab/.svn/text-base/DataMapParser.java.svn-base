package org.plab;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class DataMapParser {

	
	public static Map<String, TableObject> parseTableObject(String fileName) throws ParserConfigurationException, SAXException, IOException{
		File file = new File(fileName);
		Map<String,TableObject> retVal = new HashMap<String, TableObject>();
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		  dbf.setCoalescing(false);//Don't remove CDATA
		  DocumentBuilder db = dbf.newDocumentBuilder();
		  InputSource source = new InputSource(new InputStreamReader(new FileInputStream(file), "UTF-8"));
		  source.setEncoding("UTF-8");
		  Document doc = db.parse(source);
		  doc.getDocumentElement().normalize();
		  NodeList nodeLst = doc.getElementsByTagName("table");
		  for (int s = 0; s < nodeLst.getLength(); s++) {
			TableObject t = new TableObject();  
		  
			Node fstNode = nodeLst.item(s);
			t.tableName = ((Element)fstNode).getAttribute("name");
			t.sequenceName = ((Element)fstNode).getAttribute("sequence");
			t.readOnly = "TRUE".equalsIgnoreCase(((Element)fstNode).getAttribute("read-only"));
			t.key = ((Element)fstNode).getAttribute("primary-key");
			retVal.put(t.tableName, t);
			NodeList relationships = ((Element)fstNode).getElementsByTagName("relationship");
			for(int i=0; i< relationships.getLength(); i++){
				Node r = relationships.item(i);
				String table = ((Element)r).getAttribute("table-name");
				String fk = ((Element)r).getAttribute("foreign-key");
				t.relationships.put(table, new Relationship().setForeignKey(fk).setRelatedTable(table));
			}
			
		  }
		  return retVal;
	}
}
