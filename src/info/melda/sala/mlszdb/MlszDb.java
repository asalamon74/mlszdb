package info.melda.sala.mlszdb;

import java.net.URL;
import java.net.HttpURLConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

public class MlszDb {

    private static class MlszField {
        private String fieldName;
        private String fieldType;
        private int fieldIndex;
        
        public MlszField(String fieldName, String fieldType, int fieldIndex) {
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.fieldIndex = fieldIndex;
        }

        public String getDbFieldType() {
            return "date".equals(fieldType) ? "text" : fieldType;
        }

        public Object getValue(JSONObject obj) {
            switch( fieldType ) {
            case "date":
                return ((JSONObject)obj.get(fieldName)).getString("date");
            default:
                return obj.get(fieldName);
            }
        }
    }

    private static class MlszTable {

        private String tableName;
        private Map<String, MlszField> fields;

        public MlszTable(String tableName, Map<String,MlszField> fields) {
            this.tableName = tableName;
            this.fields = fields;
        }

        public MlszField getMlszField(String fieldName) {
            return fields.get(fieldName);
        }

        public String getCreateSql() {
            String sql="create table "+tableName+"(";
            for( String key : fields.keySet() ) {
                sql += key+" "+fields.get(key).getDbFieldType()+",";
            }
            sql = sql.substring(0, sql.length()-1)+")";
            return sql;
        }

	private void executeInsertSql(String tableName, JSONObject json, Map<String,Object> fkValues) throws SQLException {
	    String sqlPrefix = "insert into "+tableName+" (";
	    String valuesStr = " values (";
	    Iterator<String> fields = json.keys();
	    Map<Integer,Object> params = new HashMap<>();
	    int pIndex=0;
	    while( fields.hasNext() ) {
		String field = fields.next();
                MlszField mlszField = getMlszField(field);
                if( mlszField != null ) {
                    sqlPrefix+=field+",";
                    valuesStr+="?,";
                    params.put(++pIndex, mlszField.getValue(json));
                }
	    }
            if( fkValues != null ) {
                for( String key : fkValues.keySet() ) {
                    sqlPrefix+=key+",";
                    valuesStr+="?,";
                    params.put(++pIndex, fkValues.get(key));
                }

            }
	    String sql = sqlPrefix.substring(0,sqlPrefix.length()-1)+")"+valuesStr.substring(0,valuesStr.length()-1)+")";
            //	    System.out.println("sql:"+sql);
	    PreparedStatement statement = connection.prepareStatement(sql);
	    for( Integer key : params.keySet() ) {
		statement.setObject( key, params.get(key));
	    }                            
	    statement.executeUpdate();
	    statement.close();	    
	}	

	private void executeInsertSql(String tableName, JSONObject json) throws SQLException {
            executeInsertSql(tableName, json, null);
        }
    }

    private static final int SZERVEZET_MLSZ = 24;
    private static final String URL_PREFIX = "http://www.mlsz.hu/wp-content/plugins/mlszDatabank/interfaces/";
    private static Map<String,MlszTable> tablesCreated = new HashMap<>();
    private static Connection connection;

    private static String getDbTypeByClass(Object o) {
        Class c = o.getClass();
        if( c == Integer.class || c == Boolean.class) {
            return "integer";
        } else if( c == String.class ) {
            return "text";
        } else if( c == JSONArray.class ) {
            // ignore subtable
            return null;
        } else if ( o == JSONObject.NULL ) {
            // todo: check other records
            return "text";
        } else if( c == JSONObject.class ) {
            JSONObject jo = (JSONObject)o;
            if( jo.has("date") && jo.has("timezone") ) {
                return "date";
            } else {
                return null;
            }
        } else {
            System.out.println("Unknown type:"+o+" "+c);
            return null;
        }
    }

    private static void dropTable(String tableName) throws SQLException {
        Statement statement = connection.createStatement();
        statement.setQueryTimeout(30);
        statement.executeUpdate("drop table if exists "+tableName);
        statement.close();
    }        

    private static void createTableByJson(String tableName, JSONObject obj, Map<String,String> extraFields) throws SQLException {
        if( !tablesCreated.containsKey(tableName)) {

            Map<String,MlszField> fields = new HashMap<>();

            Iterator<String> i=obj.keys();
            int fieldIndex=0;
            while ( i.hasNext() ) {
                String key=i.next();
                String dbType = getDbTypeByClass(obj.get(key));                
                if( dbType != null ) {
                    fields.put( key, new MlszField(key, dbType, ++fieldIndex));
                }
            }
            if( extraFields != null ) {
                for( String key : extraFields.keySet() ) {
                    fields.put( key, new MlszField( key, extraFields.get(key), ++fieldIndex ));
                }
            }
            MlszTable table = new MlszTable(tableName, fields);            
            String sql = table.getCreateSql();
            System.out.println("sql:"+sql);
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);
            statement.executeUpdate(sql);
            statement.close();
            tablesCreated.put(tableName, table);
        }
    }

    private static void createTableByJson(String tableName, JSONObject obj) throws SQLException {
        createTableByJson(tableName, obj, null);
    }

    private static Connection readDb() {
        Connection ret = null;
        try {
            ret = DriverManager.getConnection("jdbc:sqlite:mlszdb.db");
        } catch(SQLException e) {
            System.err.println(e);
        }
        return ret;
    }

    private static String readURL(String urlName) {
        try {
            URL url = new URL(urlName);
            HttpURLConnection request = (HttpURLConnection) url.openConnection();
            request.connect();
            InputStream is = (InputStream)request.getContent();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));      
            StringBuilder sb = new StringBuilder();
            int cp;
            while ((cp = rd.read()) != -1) {
                sb.append((char) cp);
            }
            return sb.toString();
        } catch( IOException e ) {
            System.err.println("Error reading URL: "+e);
            return null;
        }
    }

    private static int getAktFord(int verseny) {
        int ret=0;
        try {
            PreparedStatement statement = connection.prepareStatement("select aktford from verseny where verseny_id=? order by szezon_id desc");
            statement.setInt(1, verseny);
            ResultSet rs = statement.executeQuery();
            if( rs.next() ) {
                ret=rs.getInt(1);
            }
            statement.close();            
        } catch( SQLException e) {
            System.err.println(e);
        } 
        return ret;
    }

    private static List<Integer> getEvadkods() {
        List<Integer> ret = new ArrayList<>();
        try {
            PreparedStatement statement = connection.prepareStatement("select evadkod from evad");
            ResultSet rs = statement.executeQuery();            
            while( rs.next() ) {
                ret.add(rs.getInt(1));
            }
            statement.close();
            return ret;
        } catch( SQLException e) {
            System.err.println(e);
            return null;
        }
    }

    private static String getString(JSONObject o, String key) {
        return o.isNull("key") ? null : o.getString(key);
    }

    private static void getDataToMatches(int verseny, int fordulo,int evad) throws SQLException {
        String content = readURL(URL_PREFIX+"getDataToMatches.php?verseny="+verseny+"&fordulo="+fordulo+"&csapat=&evad="+evad);
        JSONObject json = new JSONObject(content);
        JSONArray list = json.getJSONArray("list");
        Iterator<Object> iterator = list.iterator();
        Map<String, String> fks = new HashMap<>();
        fks.put("verseny", "integer");
        fks.put("fordulo", "integer");
        fks.put("evad", "integer");

        Map<String, Object> fkValues = new HashMap<>();
        fkValues.put("verseny", verseny);
        fkValues.put("fordulo", fordulo);
        fkValues.put("evad", evad);

        while (iterator.hasNext()) {
            JSONObject o = (JSONObject)iterator.next();
            //            System.out.println("o:"+o);
            createTableByJson("merkozesek", o, fks);
            MlszTable merkozesekTable = tablesCreated.get("merkozesek");
            merkozesekTable.executeInsertSql("merkozesek", o, fkValues );
        }                
    }

    private static void getDataToFilter(int verseny,int evad) throws SQLException {
        String url = URL_PREFIX+"getDataToFilter.php";
        if( evad == -1 ) {
            url += "?verseny="+verseny+"&szezon_id="+evad+"&evad="+evad;
        } else {
            url += "?verseny="+verseny+"&szervezet="+SZERVEZET_MLSZ+"&szezon_id="+evad+"&evad="+evad;        
        }
        String content = readURL(url);
        JSONObject json = new JSONObject(content);
        Iterator<Object> iterator;
        if( evad == -1 ) {
            JSONArray jEvad = json.getJSONArray("evad");
	    
            iterator = jEvad.iterator();
            while (iterator.hasNext()) {
                JSONObject o = (JSONObject)iterator.next();
		createTableByJson("evad", o);
		MlszTable evadTable = tablesCreated.get("evad");
		evadTable.executeInsertSql("evad", o);
		}
        } else {
            JSONArray jVerseny = json.getJSONArray("verseny");
            iterator = jVerseny.iterator();
            while (iterator.hasNext()) {
                JSONObject o = (JSONObject)iterator.next();
                createTableByJson("verseny", o);
		MlszTable versenyTable = tablesCreated.get("verseny");
		versenyTable.executeInsertSql("verseny", o);		
            }
        }
    }

    private static void getDataToMatchdata(int merkId, int evad) throws SQLException {
        String url = URL_PREFIX+"getDataToMatchdata.php?item="+merkId+"&evad="+evad;
        String content = readURL(url);
        JSONObject json = new JSONObject(content);
        createTableByJson("merkozesdata", json);
        MlszTable table = tablesCreated.get("merkozesdata");
	table.executeInsertSql("merkozesdata", json);
    }

    public static void main(String args[]) {
        connection = readDb();
        try {
            dropTable("evad");
            dropTable("verseny");
            System.out.println("Reading evad");
            getDataToFilter( -1, -1);
            System.out.println("Reading verseny");
            for( Integer evadkod : getEvadkods() ) {
                //                System.out.println("evadkod:"+evadkod);
                getDataToFilter( -1, evadkod);                
            }
            System.out.println("Reading merkozesek");
            dropTable("merkozesek");
            int aktFord = getAktFord(14967);
            aktFord = 1;
            for( int i=1; i<=aktFord; ++i ) {
                getDataToMatches(14967,i,15);
            }
            System.out.println("Reading merkozesdata");
            dropTable("merkozesdata");
            getDataToMatchdata(1141642,15);
        } catch( SQLException e ) {
           System.err.println(e);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    System.err.println("Cannot close database "+e);
                }
            }
        }    
    }    
}
