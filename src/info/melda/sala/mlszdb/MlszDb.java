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

    private static void createDb() {
        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:mlszdb.db");
            Statement statement = connection.createStatement();
            statement.setQueryTimeout(30);  // set timeout to 30 sec.
            
            statement.executeUpdate("drop table if exists evad");
            statement.executeUpdate("create table evad (evadkod integer, evadnev string, akutalis integer)"); // akutalis: typo in json
        } catch (SQLException e) {       
            System.err.println(e);
        } finally {
            try {
                if(connection != null) {
                    connection.close();
                }
            }
            catch(SQLException e) {
                System.err.println(e);
            }
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

    private static void jsonParse(String jsonStr, Connection conn) throws SQLException {
        JSONObject json = new JSONObject(jsonStr);
        //        System.out.println(json);
        JSONArray evad = json.getJSONArray("evad");
        System.out.println(evad);
        Iterator<Object> iterator = evad.iterator();
        while (iterator.hasNext()) {
            JSONObject o = (JSONObject)iterator.next();
            PreparedStatement statement = conn.prepareStatement("insert into evad (evadkod, evadnev, akutalis) values(?,?,?)");
            statement.setInt(1, o.getInt("evadkod"));
            statement.setString(2, o.getString("evadnev"));
            statement.setInt(3, o.getInt("akutalis"));
            statement.executeUpdate();
            System.out.println(o);
        }
    }

    public static void main(String args[]) {
        createDb();
        Connection conn = readDb();
        try {
            jsonParse(readURL("http://www.mlsz.hu/wp-content/plugins/mlszDatabank/interfaces/getDataToFilter.php"), conn);
        } catch( SQLException e ) {
           System.err.println(e);

        }
    }

}
