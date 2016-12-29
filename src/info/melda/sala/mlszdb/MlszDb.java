package info.melda.sala.mlszdb;

import java.net.URL;
import java.net.HttpURLConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Iterator;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONArray;

public class MlszDb {

    private int SZERVEZET_MLSZ = 24;
    private static final String URL_PREFIX = "http://www.mlsz.hu/wp-content/plugins/mlszDatabank/interfaces/";

    private static String getDbTypeByClass(Class c) {
        if( c == Integer.class ) {
            return "integer";
        } else if( c == String.class ) {
            return "string";
        } else {
            return null;
        }
    }

    private static void createTableByJson(Connection conn, String tableName, JSONObject obj) throws SQLException {
        Iterator<String> i=obj.keys();
        String sql="create table "+tableName+"(";
        while ( i.hasNext() ) {
            String key=i.next();
            sql += key+" "+getDbTypeByClass(obj.get(key).getClass())+",";
        }
        sql = sql.substring(0, sql.length()-1)+")";
        Statement statement = conn.createStatement();
        statement.setQueryTimeout(30);
            
        statement.executeUpdate("drop table if exists "+tableName);
        statement.executeUpdate(sql);
    }

    private static Connection readDb() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:mlszdb.db");
        } catch(SQLException e) {
            System.err.println(e);
        }
        return connection;
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

    private static void jsonParse(String jsonStr, Connection conn) throws SQLException {
        JSONObject json = new JSONObject(jsonStr);
        JSONArray evad = json.getJSONArray("evad");
        Iterator<Object> iterator = evad.iterator();
        boolean first=true;
        while (iterator.hasNext()) {
            JSONObject o = (JSONObject)iterator.next();
            if( first ) {
                createTableByJson(conn, "evad", o);
                first=false;
            }
            PreparedStatement statement = conn.prepareStatement("insert into evad (evadkod, evadnev, akutalis) values(?,?,?)");
            statement.setInt(1, o.getInt("evadkod"));
            statement.setString(2, o.getString("evadnev"));
            statement.setInt(3, o.getInt("akutalis"));
            statement.executeUpdate();
        }

        JSONArray verseny = json.getJSONArray("verseny");
        //        System.out.println(verseny);
        iterator = verseny.iterator();
        first=true;
        while (iterator.hasNext()) {
            JSONObject o = (JSONObject)iterator.next();
            if( first ) {
                createTableByJson(conn, "verseny", o);
                first=false;
            }
            PreparedStatement statement = conn.prepareStatement("insert into verseny (id,verseny_id, nev, szerv_id, szezon_id,kieg_igazolas_tipus_kod, szint, bajnkupa, szakag, spkod,ferfi_noi, jvszekod, jv_kikuld, versenyrendszer_id, korosztaly,web_nev, lathato, nupi, szam_kezdo, szam_csere,ervenyes,aktford) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            statement.setInt(1, o.getInt("id"));
            statement.setInt(2, o.getInt("verseny_id"));
            statement.setString(3, o.getString("nev"));
            statement.setInt(4, o.getInt("szerv_id"));
            statement.setInt(5, o.getInt("szezon_id"));
            statement.setInt(6, o.getInt("kieg_igazolas_tipus_kod"));
            statement.setInt(7, o.getInt("szint"));
            statement.setInt(8, o.getInt("bajnkupa"));
            statement.setInt(9, o.getInt("szakag"));
            statement.setInt(10, o.getInt("spkod"));
            statement.setInt(11, o.getInt("ferfi_noi"));
            statement.setInt(12, o.getInt("jvszekod"));
            statement.setInt(13, o.getInt("jv_kikuld"));
            statement.setInt(14, o.getInt("versenyrendszer_id"));
            statement.setInt(15, o.getInt("korosztaly"));
            statement.setString(16, o.getString("web_nev"));
            statement.setInt(17, o.getInt("lathato"));
            statement.setInt(18, o.getInt("nupi"));
            statement.setInt(19, o.getInt("szam_kezdo"));
            statement.setInt(20, o.getInt("szam_csere"));
            statement.setInt(21, o.getInt("ervenyes"));
            statement.setInt(22, o.getInt("aktford"));
            statement.executeUpdate();
            //            System.out.println(o);
        }
    }

    public static void main(String args[]) {
        //        createDb();
        Connection conn = readDb();
        try {
            jsonParse(readURL(URL_PREFIX+"getDataToFilter.php"), conn);
        } catch( SQLException e ) {
           System.err.println(e);

        }
    }

}
