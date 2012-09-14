package org.plab;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;



import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;
import java.io.FileNotFoundException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionManager {

    public static Set<String> JDBC_DRIVER = new HashSet<String>(Arrays.asList(new String[]{"oracle.jdbc.driver.OracleDriver"}));//, "com.mysql.jdbc.Driver"
    public static String PROPERTY_FILE = "C:/connections.properties";
    public static String CONFIG_URL = "ftp://dcahalane:jae5Ucia@myoffice.photronics.com/misc/" + PROPERTY_FILE;
    public static String PROPERTY_DIR = "";
    
    static ConnectionManager INSTANCE;
    static Map<String, BoneCP> DATA_SOURCES = new HashMap<String, BoneCP>();
    Map<String, Connection> CONNECTIONS = new HashMap<String, Connection>();
    Properties properties;

    public ConnectionManager() {
        String prop = System.getProperty("PROPERTY_URL");
        if (prop != null) {
            CONFIG_URL = prop;
        }
        prop = null;
        prop = System.getProperty("PROPERTY_FILE");
        if (prop != null) {
            PROPERTY_FILE = prop;
        }
        prop = null;
        prop = System.getProperty("JDBC_DRIVER");
        if (prop != null) {
            JDBC_DRIVER.add(prop);
        }
        try {
            initializeProperties();
        } catch (IOException ex) {
            Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("*****ORMjs Connection Manager");
        System.out.println("*****DRIVER\t" + JDBC_DRIVER);
        System.out.println("*****PROPERTY_FILE\t" + PROPERTY_FILE);
        System.out.println("*****PROPERTY_DIR\t" + PROPERTY_DIR);
        System.out.println("*****PROPERTY_URL\t" + CONFIG_URL);
    }

    public void initializeProperties() throws FileNotFoundException, IOException {
        System.out.println("Initializing Properties");

        properties = getProperties();
        Field[] fields = ConnectionManager.class.getFields();
        for (int i = 0; i < fields.length; i++) {
            int mod = fields[i].getModifiers();
            if (Modifier.isStatic(mod) && Modifier.isPublic(mod) && !Modifier.isFinal(mod)) {
                String name = fields[i].getName();
                String value = properties.getProperty(name);
                if (value != null) {
                    try {
                        System.out.println("***Setting " + name + " to " + value);
                        fields[i].set(null, value);
                    } catch (Exception e) {
                        System.out.println("Unable to set ConnectionManager static properties from file for Field name:" + name);
                        Logger.getLogger(ConnectionManager.class.getName()).log(Level.INFO, null, e);
                    }
                }
            }

        }
    }
    
        public Properties getProperties() throws IOException {
        if (properties == null) {
            properties = new Properties();
            try {
                if (PROPERTY_FILE == null) {
                    throw new Exception("Properties not defined");
                }
                properties.load(new FileInputStream(new File( PROPERTY_DIR, PROPERTY_FILE)));

            } catch (Exception e) {
                System.out.println("Could not load from " + PROPERTY_FILE);
                Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, e);
                try {
                    URL url = new URL(CONFIG_URL);
                    URLConnection connection = url.openConnection();
                    properties.load(connection.getInputStream());
                } catch (Exception eUrl) {
                    Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, eUrl);
                }
            }
        }
        return properties;
    }

        public void setProperties(Properties property){
            this.properties = property;
        try {
            initializeProperties();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ConnectionManager.class.getName()).log(Level.SEVERE, null, ex);
        }
        }
        
        
    public static ConnectionManager getInstance() {

        if (INSTANCE == null) {
            INSTANCE = new ConnectionManager();
        }
        return INSTANCE;
    }

    public Connection getConnection(String connectionName) throws IOException, SQLException {
        Connection dataSource = CONNECTIONS.get(connectionName);

        Connection retVal = null;
        if (dataSource == null || dataSource.isClosed()) {
            String url = properties.getProperty(connectionName);
System.out.println(connectionName + "\t" + url);
            for(String driver: JDBC_DRIVER){
                
                    try{
                        Logger.getLogger(ConnectionManager.class.getName()).log(Level.INFO, "Attempting to load driver " + driver);
                        Class.forName(driver);
                    }catch(Exception eDrv){
                        Logger.getLogger(ConnectionManager.class.getName()).log(Level.INFO, "Could not load driver " + driver);
                    }
            }
            
            //ObjectPool connectionPool = new GenericObjectPool(null);
            //ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(getProperties().getProperty(connectionName),null);
            //PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory,connectionPool,null,null,false,true);
            //dataSource = new PoolingDataSource(connectionPool);
            //BoneCPConfig config = new BoneCPConfig();
            //config.setJdbcUrl(url);
            dataSource = DriverManager.getConnection(url);
            //ds.setUrl(getProperties().getProperty(connectionName));
            CONNECTIONS.put(connectionName, dataSource);
        }
        retVal = dataSource;

        if (retVal == null || retVal.isClosed()) {
            CONNECTIONS.put(connectionName, null);
            System.out.println("Connection closed. Attempting to reopen connection");
            retVal = getConnection(connectionName);
        }

        return retVal;
    }
    


}
