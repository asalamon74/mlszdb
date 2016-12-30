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

    private static final int SZERVEZET_MLSZ = 24;
    private static final String URL_PREFIX = "http://www.mlsz.hu/wp-content/plugins/mlszDatabank/interfaces/";
    private static Set<String> tablesCreated = new HashSet<String>();

    private static String getDbTypeByClass(Object o) {
        Class c = o.getClass();
        if( c == Integer.class ) {
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
            // dates 
            // TODO: better checking
            return "text";
        } else {
            //            System.out.println("c:"+c);
            return null;
        }
    }

    private static void dropTable(Connection conn, String tableName) throws SQLException {
        Statement statement = conn.createStatement();
        statement.setQueryTimeout(30);
            
        statement.executeUpdate("drop table if exists "+tableName);
    }        

    private static void createTableByJson(Connection conn, String tableName, JSONObject obj) throws SQLException {
        if( !tablesCreated.contains(tableName)) {
            Iterator<String> i=obj.keys();
            String sql="create table "+tableName+"(";
            while ( i.hasNext() ) {
                String key=i.next();
                //                System.out.println("key:"+key);
                String dbType = getDbTypeByClass(obj.get(key));
                if( dbType != null ) {
                    sql += key+" "+dbType+",";
                }
            }
            sql = sql.substring(0, sql.length()-1)+")";
            Statement statement = conn.createStatement();
            statement.setQueryTimeout(30);
            
            //   statement.executeUpdate("drop table if exists "+tableName);
            statement.executeUpdate(sql);
            tablesCreated.add(tableName);
        }
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

    private static int getAktFord(Connection conn, int verseny) {
        try {
            PreparedStatement statement = conn.prepareStatement("select aktford from verseny where verseny_id=?");
            statement.setInt(1, verseny);
            ResultSet rs = statement.executeQuery();
            if( rs.next() ) {
                return rs.getInt(1);
            }
            return 0;
        } catch( SQLException e) {
            System.err.println(e);
            return 0;
        }      
    }

    private static String getString(JSONObject o, String key) {
        return o.isNull("key") ? null : o.getString(key);
    }

    private static void getDataToMatches(Connection conn, int verseny, int fordulo,int evad) throws SQLException {
        String content = readURL(URL_PREFIX+"getDataToMatches.php?verseny="+verseny+"&fordulo="+fordulo+"&csapat=&evad="+evad);
        JSONObject json = new JSONObject(content);
        JSONArray list = json.getJSONArray("list");
        Iterator<Object> iterator = list.iterator();
        while (iterator.hasNext()) {
            JSONObject o = (JSONObject)iterator.next();
            //            System.out.println("o:"+o);
            createTableByJson(conn, "merkozesek", o);

            PreparedStatement statement = conn.prepareStatement("insert into merkozesek (hegkod, lejatszva, vegkod, hcsapat_id, vcsapat_id, merk_id, stadion_nev, stadion_varos, merk_ido, datumteny, hgol, vgol, hazai_csapat, vendeg_csapat, hazai_logo_url, vendeg_logo_url) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            //            statement.setInt(1, o.getInt("hbuntgol"));
            //            statement.setInt(2, o.getInt("vbuntgol"));
            statement.setInt(1, o.getInt("hegkod"));
            statement.setInt(2, o.getInt("lejatszva"));
            statement.setInt(3, o.getInt("vegkod"));
            statement.setInt(4, o.getInt("hcsapat_id"));
            statement.setInt(5, o.getInt("vcsapat_id"));
            statement.setInt(6, o.getInt("merk_id"));
            statement.setString(7, o.getString("stadion_nev"));
            statement.setString(8, o.getString("stadion_varos"));
            statement.setString(9, o.getString("merk_ido"));
            statement.setString(10, ((JSONObject)o.get("datumteny")).getString("date"));
            statement.setInt(11, o.getInt("hgol"));
            statement.setInt(12, o.getInt("vgol"));
            statement.setString(13, o.getString("hazai_csapat"));
            statement.setString(14, o.getString("vendeg_csapat"));
            statement.setString(15, getString(o, "hazai_logo_url"));
            statement.setString(16, getString(o, "vendeg_logo_url"));
            statement.executeUpdate();
        }                
    }

    private static void getDataToFilter(Connection conn, int verseny,int evad) throws SQLException {
        String url = URL_PREFIX+"getDataToFilter.php";
        if( verseny == -1 ) {
            url += "?verseny="+verseny+"&szezon_id="+evad+"&evad="+evad;
        } else {
            url += "?verseny="+verseny+"&szervezet="+SZERVEZET_MLSZ+"&szezon_id="+evad+"&evad="+evad;        
        }
        String content = readURL(url);
        JSONObject json = new JSONObject(content);
        JSONArray jEvad = json.getJSONArray("evad");
        Iterator<Object> iterator = jEvad.iterator();
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

        JSONArray jVerseny = json.getJSONArray("verseny");
        //        System.out.println(jVerseny);
        iterator = jVerseny.iterator();
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
        Connection conn = readDb();
        try {
            dropTable(conn, "evad");
            dropTable(conn, "verseny");
            getDataToFilter( conn, -1, -1);
            dropTable(conn, "merkozesek");
            int aktFord = getAktFord(conn, 14967);
            for( int i=1; i<=aktFord; ++i ) {
                getDataToMatches(conn,14967,i,15);
            }
        } catch( SQLException e ) {
           System.err.println(e);

        }
    }

}
