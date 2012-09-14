/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.plab.node;

import java.io.IOException;
import java.sql.SQLException;
import org.plab.Converter;

/**
 *
 * @author dcahalane
 */
public class NodeORM {
    
    public String driver;
    public String connectionName;
    public String dataMapLocation;
    public Converter converter;
    
    public NodeORM(){
        
    }
    
    public void init(String driver, String connectionName, String dataMap, String propertyFile){
        this.driver = driver;
        this.connectionName = connectionName;
        this.dataMapLocation = dataMap;
        initProperties(propertyFile, driver);
        converter = new Converter(dataMap, connectionName);
    }
    
    void initProperties(String propertyFile, String driver){
        System.setProperty("PROPERTY_FILE", propertyFile);
        System.setProperty("JDBC_DRIVER", driver);
        
    }
    
    public String search(String tableName, String key, String value) throws IOException, SQLException, ClassNotFoundException {
        String json = converter.search(tableName, key, value);
        return json;
    }
    
    public String retrieve(String tableName, String key) throws IOException, SQLException, ClassNotFoundException {
        System.out.println("Retrieving " + tableName + " with primary-key of " + key);
        String json = converter.retrieve(tableName, key, true);
        return json;
    }
    
    public String create(String tableName) throws IOException, SQLException {
        String json = null;
        if (tableName != null) {
             json = converter.create(tableName);
        }
        return json;
    }

    public String update(String json) throws IOException {
        String retVal = null;
        if (json != null) {
            try {
                retVal = converter.update(json);
            } catch (Exception ex) {
                System.out.println("Error Updating JSON " + ex.getMessage());
            }
        }
        return retVal;
    }
    
    
}
