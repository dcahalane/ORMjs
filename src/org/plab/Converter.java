package org.plab;

import java.io.IOException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.ArrayList;
import java.util.Properties;

public class Converter {

    static Map<String, TableObject> TABLE_MAP = new HashMap<String, TableObject>();
    String databaseName;

    public Converter(String dataMapFile) {
        initializeMap(dataMapFile);
    }

    public Converter(String dataMapFile, String databaseName) {
        this(dataMapFile);
        this.databaseName = databaseName;
    }
    
    public Converter(String dataMapFile, String databaseName, Properties properties){
        this(dataMapFile, databaseName);
        ConnectionManager.getInstance().setProperties(properties);
    }

    public void initializeMap(String dataMapFile) {
        if (TABLE_MAP.size() < 1) {
            try {
                TABLE_MAP = DataMapParser.parseTableObject(dataMapFile);
            } catch (ParserConfigurationException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (SAXException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            //TODO: parse XML File and build up the collection of TableObjects, TableFields and Relationships
        }
    }

    public void refreshDataMap(String dataMapFile) {
        TABLE_MAP.clear();
        initializeMap(dataMapFile);
    }

    public TableObject getDefaultObject(String tableName) {
        TableObject retVal = TABLE_MAP.get(tableName);
        if (retVal != null) {
            retVal = retVal.copy();
        }
        return retVal;
    }

    public void delete(String json) {
    }

    public String updateField(String json, String fieldName, String value) {
        Gson gson = new Gson();
        TableObject obj = gson.fromJson(json, TableObject.class);
        TableField field = obj.fields.get(fieldName);
        if (field != null) {
            field.setValue(value);
        }
        return gson.toJson(obj);
    }
    
    public String setFieldValue(String json, String fieldName, String value){
        return updateField(json, fieldName, value);
    }

    public String getFieldValue(String json, String fieldName) {
        String retVal = null;
        Gson gson = new Gson();
        TableObject obj = gson.fromJson(json, TableObject.class);
        TableField field = obj.fields.get(fieldName);
        if (field != null) {
            retVal = field.value;
        }
        return retVal;
    }

    public String update(String json) throws ClassNotFoundException, SQLException, IOException {
        Gson gson = new Gson();
        TableObject obj = gson.fromJson(json, TableObject.class);
        obj = update(obj, null);
        return gson.toJson(obj);
    }

    TableObject update(TableObject table, TableObject parent) throws ClassNotFoundException, SQLException, IOException {
        Connection con = null;
        Statement stmt = null;
        try {
            con = ConnectionManager.getInstance().getConnection(databaseName);
            stmt = con.createStatement();
            if (table.keyValue != null && !table.readOnly) {

                ResultSet rs = stmt.executeQuery(buildSelectString(table));
                if (!rs.next()) {//If false Create a new one.
                    rs = stmt.executeQuery(buildSequenceQuery(table));
                    if (rs.next()) {
                        table.setKeyValue(rs.getString(1));
                        insert(table, parent);
                    }
                } else if (table.keyValue != null) {
                    StringBuffer updateBuffer = new StringBuffer();
                    for (String fieldName : table.fields.keySet()) {
                        if (updateBuffer.length() > 1) {
                            updateBuffer.append(", ");
                        }
                        updateBuffer.append(fieldName + " = ");
                        updateBuffer.append(convertValue(table.fields.get(fieldName)));
                    }
                    updateBuffer.insert(0, "UPDATE " + table.tableName + " SET ");
                    //Handle Reverse Mapping for relationships
                    if (parent != null) {
                        Relationship r = parent.relationships.get(table.tableName);
                        if (r != null) {
                            if (updateBuffer.length() > 1) {
                                updateBuffer.append(", ");
                            }
                            updateBuffer.append(r.foreignKey + " = " + parent.keyValue);
                        }
                    }
                    updateBuffer.append(" WHERE " + table.key + " = " + table.keyValue);
//System.out.println(updateBuffer.toString());
                    stmt.executeUpdate(updateBuffer.toString());

                }
            } else if (table.keyValue == null && !table.readOnly) {
                table = insert(table, parent);
            }


            for (Relationship r : table.relationships.values()) {
                /* TODO: Handle deletion of children.
                 * Get the primary-key values for this relationship.
                 * If the child exists with that primary-key, then update it.
                 * If no child exists with that primary-key, then delete it.
                 */
                String selectChildren = buildRelationshipSelectString(table, r);
//System.out.println(selectChildren);
                ResultSet rsChildren = stmt.executeQuery(selectChildren);
                List<String> existingChildren = new LinkedList<String>();
                while (rsChildren.next()) {
                    existingChildren.add(rsChildren.getString(1));
                }
                Iterator<TableObject> iChild = r.children.iterator();
                while (iChild.hasNext()) {
                    TableObject child = iChild.next();
                    if (existingChildren.contains(child.keyValue)) {
                        update(child, table);
                        existingChildren.remove(child.keyValue);
                    } else if (child.keyValue == null) {
                        insert(child, table);
                    }
                }
                TableObject relatedTable = TABLE_MAP.get(r.relatedTable);
                if (existingChildren.size() > 0 && !relatedTable.readOnly) {
                    String deleteQry = buildDeleteString(r, existingChildren);
//System.out.println(deleteQry);
                    stmt.execute(deleteQry);
                }
            }
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (con != null) {
                try {
                    //con.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        }
        return table;
    }

    TableObject insert(TableObject table, TableObject parent) throws ClassNotFoundException, SQLException, IOException {
        Connection con = null;
        Statement stmt = null;
        try {
            con = ConnectionManager.getInstance().getConnection(databaseName);
            stmt = con.createStatement();
            String seqQry = buildSequenceQuery(table);
            String nextVal = null;
            ResultSet rsSeq = stmt.executeQuery(seqQry);
            if (rsSeq.next()) {
                nextVal = rsSeq.getString(1);
            }
            table.setKeyValue(nextVal);
            StringBuffer insertBuffer = new StringBuffer();
            StringBuffer valueBuffer = new StringBuffer();
            for (String fieldName : table.fields.keySet()) {
                if (insertBuffer.length() > 1) {
                    insertBuffer.append(", ");
                    valueBuffer.append(", ");
                }
                insertBuffer.append(fieldName);
                if (!table.key.equals(fieldName)) {
                    valueBuffer.append(convertValue(table.fields.get(fieldName)));
                } else {
                    valueBuffer.append(table.keyValue);
                }
            }
            if (parent != null) {
                Relationship r = parent.relationships.get(table.tableName);
                if (r != null) {
                    if (insertBuffer.length() > 1) {
                        insertBuffer.append(", ");
                        valueBuffer.append(", ");
                    }
                    insertBuffer.append(r.foreignKey);
                    valueBuffer.append(parent.keyValue);
                }
            }

            insertBuffer.insert(0, "INSERT INTO " + table.tableName + "( ");
            insertBuffer.append(")");
            valueBuffer.insert(0, " VALUES (");
            valueBuffer.append(")");
//System.out.println(insertBuffer.toString() + valueBuffer.toString());
            if (!table.readOnly) {
                stmt.executeUpdate(insertBuffer.toString() + valueBuffer.toString());
            }
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (con != null) {
                try {
                    //con.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        }
        return table;
    }

    public String retrieve(String tableName, String keyValue) throws SQLException, ClassNotFoundException, IOException {
        return retrieve(tableName, keyValue, false);
    }

    public String retrieve(String tableName, String keyValue, boolean deep) throws SQLException, ClassNotFoundException, IOException {
        return retrieve(getDefaultObject(tableName).setKeyValue(keyValue), deep);
    }

    public String retrieve(TableObject object, boolean deep) throws SQLException, ClassNotFoundException, IOException {
        GsonBuilder builder = new GsonBuilder();
        Gson gson = builder.create();
        object = (object.keyValue != null) ? _retrieve(object, deep, null) : object;
        String retVal = gson.toJson(object);
        return retVal;
    }

    TableObject _retrieve(TableObject table, boolean deep, String fkCol) throws SQLException, ClassNotFoundException, IOException {
        Connection con = null;
        Statement stmt = null;
        try {

            con = ConnectionManager.getInstance().getConnection(databaseName);

            stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery(buildSelectString(table));

            ResultSetMetaData rsmd = rs.getMetaData();
            boolean filterColumns = table.fields.size() > 0;
            if (rs.next()) {
                if (filterColumns) {
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        String colName = rsmd.getColumnName(i);
                        if (table.fields.keySet().contains(colName)) {
                            table.fields.put(colName, new TableField().setValue(rs.getString(i)).setType(rsmd.getColumnTypeName(i)));
                        }
                    }
                } else {
                    for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                        if (!rsmd.getColumnName(i).equals(fkCol)) {
                            table.fields.put(rsmd.getColumnName(i), new TableField().setValue(rs.getString(i)).setType(rsmd.getColumnTypeName(i)));//Set Label
                        }
                    }
                }
            }

            for (String t : table.relationships.keySet()) {
                if (rs != null) {
                    rs.close();
                }
                rs = null;
                if (stmt != null) {
                    stmt.close();
                }
                stmt = null;

                //con = ConnectionManager.getInstance().getConnection(databaseName);
                stmt = con.createStatement();

                Relationship r = table.relationships.get(t);
                TableObject childMaster = TABLE_MAP.get(t);
                String pkColumn = childMaster.key;
                rs = stmt.executeQuery("SELECT " + pkColumn + " FROM " + t + " WHERE " + r.foreignKey + " = " + table.keyValue);

                while (rs.next()) {
                    TableObject child = childMaster.copy();
                    child.setKeyValue(rs.getString(1));
                    if (deep) {
                        r.addChildObject(_retrieve(child, deep, r.foreignKey));
                    } else {
                        r.addChildObject(child);
                    }
                }
            }
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (con != null) {
                try {
                    //con.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        }
        return table;
    }

    
    /*
     * keyValue is an array of KEY=VALUE where VALUE can be a comma delimited list of values
     */
    public String[] search(String tableName, String[] keyValue) {
        List<String> retVal = new ArrayList<String>();
        Connection con = null;
        Statement stmt = null;
        try {
            con = ConnectionManager.getInstance().getConnection(databaseName);
            stmt = con.createStatement();
            TableObject table = _create(tableName);
            if (table != null) {
                StringBuffer whereClause = new StringBuffer();
                for (int i = 0; i < keyValue.length; i++) {
                    String[] kV = keyValue[i].split("=");
                    if (whereClause.length() < 1) {
                        whereClause.append(" AND ");
                    }
                    int multi = kV[1].indexOf(",");
                    boolean isChar = false;
                    TableField field = table.fields.get(kV[0]);
                    isChar = field.fieldType.startsWith("VARCHAR");
                    if(multi < 0){
                        if(isChar){
                            whereClause.append( kV[0] + " LIKE '%" + kV[1] + "%' ");
                        }else{
                            field.setValue(kV[1]);
                            whereClause.append(kV[0] + " = " + convertValue(field));
                        }
                    }else{
                        whereClause.append(kV[0] + " in (" + kV[0] + ") ");
                    }
                }
                String searchQry = "SELECT " + table.key + " FROM " + tableName + " WHERE " + whereClause.toString();
                ResultSet rs = stmt.executeQuery(searchQry);
                while (rs.next()) {
                    String keyVal = rs.getString(1);
                    retVal.add(retrieve(tableName, keyVal));
                }
            }

        } catch (Exception e) {
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (con != null) {
                try {
                    //con.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        }
        return (String[])retVal.toArray();
    }

    public String search(String tableName, String fieldName, String val) {
        Connection con = null;
        Statement stmt = null;
        try {
            con = ConnectionManager.getInstance().getConnection(databaseName);
            stmt = con.createStatement();
            TableObject table = _create(tableName);
            if (table != null) {
                String searchQry = "SELECT " + table.key + " FROM tableName WHERE " + fieldName + " = " + val;
                TableField field = table.fields.get(fieldName);
                if (field != null) {
                    field.setValue(val);
                    searchQry = "SELECT " + table.key + " FROM tableName WHERE " + fieldName + " = " + convertValue(field);
                }
                ResultSet rs = stmt.executeQuery(searchQry);
                if (rs.next()) {
                    String keyVal = rs.getString(1);
                    return retrieve(tableName, keyVal);
                }
            }
        } catch (Exception e) {
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
            if (con != null) {
                try {
                    //con.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }
            }
        }
        return null;
    }

    String buildSelectString(TableObject table) {
        return "SELECT * FROM " + table.tableName + " WHERE " + table.key + " = " + table.keyValue;
    }

    String buildRelationshipSelectString(TableObject t, Relationship r) {
        StringBuffer retVal = new StringBuffer();
        TableObject rTable = TABLE_MAP.get(r.relatedTable);
        retVal.append("SELECT " + rTable.key + " FROM " + r.relatedTable + " WHERE " + r.foreignKey + " = " + t.keyValue);
        return retVal.toString();
    }

    String buildDeleteString(Relationship r, List<String> keys) {
        StringBuffer retVal = new StringBuffer();
        TableObject rTable = TABLE_MAP.get(r.relatedTable);

        for (String s : keys) {
            if (retVal.length() > 0) {
                retVal.append(", ");
            }
            retVal.append(s);
        }
        retVal.insert(0, "DELETE " + r.relatedTable + " WHERE " + rTable.key + " in (");
        retVal.append(")");
        return retVal.toString();
    }

    String buildSequenceQuery(TableObject table) {
        if (table.sequenceName != null && table.sequenceName.length() > 0) {
            return "SELECT " + table.sequenceName + ".NEXTVAL FROM DUAL";
        } else {
            return "SELECT NEXT_VAL('" + table.tableName + "') FROM DUAL";//This handles mySQL type function based sequences.
        }
    }

    public String convertValue(TableField field) {
        StringBuffer retVal = new StringBuffer();
        if (field.value == null) {
            return field.value;
        }

        if (field.fieldType.startsWith("VARCHAR")) {
            retVal.append("'" + field.value + "'");
        } else if (field.fieldType.equals("DATE")) {
            //TODO: Handle different date formats coming back from the client. ie no timestamp or no seconds....
            String value = field.value;
            if (value != null) {
                int s = value.lastIndexOf(".");
                if (s == value.length() - 2) {
                    value = value.substring(0, s);
                }
                retVal.append("TO_DATE('" + value + "', 'YYYY-MM-DD HH24:MI:SS')");
            } else {
                retVal.append(field.value);
            }
        } else {
            retVal.append(field.value);
        }
        return retVal.toString();
    }

    TableObject _create(String tableName) throws IOException, SQLException {
        TableObject table = getDefaultObject(tableName);
        Connection con = null;
        Statement stmt = null;
        try {
            con = ConnectionManager.getInstance().getConnection(databaseName);
            stmt = con.createStatement();
            ResultSet rs = null;
            //TODO: This should be based on the driver being used for this db connection.
            try {
                rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE ROWNUM = 1");
            } catch (Exception e) {
                rs = stmt.executeQuery("SELECT * FROM " + tableName + " limit 0, 1");//Handle mySQL.
            }
            ResultSetMetaData rsmd = rs.getMetaData();
            //boolean filterColumns = table.fields.size() > 0;
            if (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    table.fields.put(rsmd.getColumnName(i), new TableField().setType(rsmd.getColumnTypeName(i)));//Set Label
                }

            }
        } finally {
            if (con != null) {
                try {
                    //con.close();
                } catch (Exception e) {
                    Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, e);
                }

            }
        }
        TABLE_MAP.put(tableName, table);
        return table;
    }

    public String create(String tableName) throws IOException, SQLException {
        TableObject table = _create(tableName);
        //TODO: Run the sequence generator for this object and set its key-value.
        Gson gson = new Gson();
        String retVal = gson.toJson(table);
        return retVal;
    }

    public static void main(String[] args) {
        try {
            /*
             * Converter converter = new Converter("c:/data-map.xml");
             * String json = converter.retrieve("MESSAGE","1234");
             * --
             * json = converter.update(json);
             * json = converter.create("MESSAGE");
             * 
             * If using mySQL with sequences, you must create a Function
             * called NEXT_VAL(pTable varchar2) that returns the tables 
             * sequence.
             */
            //Initialize the ConnectionManager
            //You must initialize where the PROPERTY_FILE is located.
            ConnectionManager.PROPERTY_FILE = "c:/connections.properties";
            //ConnectionManager.JDBC_DRIVER.add(""); mySQL and Oracle are included by default.
            Converter c = new Converter("C:/jobs-map.xml", "BRKDVLP");
            String json;
            try {
                json = c.retrieve("JOBS", "2887", true);
                System.out.println(json);
                //json = c.retrieve("CONTACT", "1001", false);
                //System.out.println(json);
                //c.update(json);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            //String newContact = "{\"tableName\":\"CONTACT\",\"key\":\"CNT_ID\",\"fields\":{\"FIRST_NAME\":{\"value\":\"Joe\",\"fieldType\":\"VARCHAR\"},\"ENT_ID_FK\":{\"fieldType\":\"INT\"},\"EMAIL\":{\"value\":\"jglotz@photronics.com\",\"fieldType\":\"VARCHAR\"},\"PASSWORD\":{\"fieldType\":\"VARCHAR\"},\"OFFICE_PHONE\":{\"value\":\"203-740-5309\",\"fieldType\":\"VARCHAR\"},\"LAST_NAME\":{\"value\":\"Glotz\",\"fieldType\":\"VARCHAR\"},\"CNT_ID\":{\"fieldType\":\"INT\"}},\"relationships\":{\"CONTACT_DOMAIN\":{\"children\":[],\"relatedTable\":\"CONTACT_DOMAIN\",\"foreignKey\":\"CNT_ID_FK\"},\"CONTACT_CONTACT\":{\"children\":[],\"relatedTable\":\"CONTACT_CONTACT\",\"foreignKey\":\"CNT_ID_FK\"}},\"readOnly\":\"false\",\"sequenceName\":\"\"}";
            //c.update(newContact);
        } catch (Exception ex) {
            Logger.getLogger(Converter.class.getName()).log(Level.SEVERE, null, ex);
        }


    }
}
