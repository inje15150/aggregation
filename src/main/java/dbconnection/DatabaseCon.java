package dbconnection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DatabaseCon {

    private final static Logger log = LogManager.getLogger(DatabaseCon.class);
    private final static String DRIVER = "org.mariadb.jdbc.Driver";
    private final static String URL = "jdbc:mariadb://192.168.60.82:3306/mariadb";
    private final static String userID = "root";
    private final static String PASSWORD = "naim4321";

    static Connection con = null;
    static PreparedStatement pst = null;
    static ResultSet rs = null;

    public void connection() {
        try {
            Class.forName(DRIVER);
            con = DriverManager.getConnection(URL, userID, PASSWORD);
            log.info("mariaDB connection success");

        } catch (ClassNotFoundException | SQLException e) {
            log.error(e.getMessage());
        }
    }

    public Map<String, String> selectIpAndName() {
        Map<String, String> ipAndName = new ConcurrentHashMap<>();
        try {
            String query = "SELECT * FROM agent_info_list";

            pst = con.prepareStatement(query);
            rs = pst.executeQuery();

            while (rs.next()) {
                String ip = rs.getString("ip");
                String name = rs.getString("name");
                ipAndName.put(ip, name);
            }
            return ipAndName;
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return ipAndName;
    }

    public Map<String, String> selectIpAndKey() {
        Map<String, String> keys = new ConcurrentHashMap<>();
        try {
            String query = "SELECT * FROM agent_info_list";

            pst = con.prepareStatement(query);
            rs = pst.executeQuery();

            while (rs.next()) {
                String id = rs.getString("id");
                String ip = rs.getString("ip");
                keys.put(ip, id);
            }
            return keys;

        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return keys;
    }

    public List<String> selectIpList() {
        List<String> keys = new ArrayList<>();
        try {
            String query = "SELECT * FROM agent_info_list";

            pst = con.prepareStatement(query);
            rs = pst.executeQuery();

            while (rs.next()) {
                String ip = rs.getString("ip");
                keys.add(ip);
            }
            return keys;
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return keys;
    }

    // rules insert
    public void rules_insert(int agent_type, int duration_time, int count, int value) {
        try {
            String query = "INSERT INTO rules (agent_type, duration_time, count, value) values (?,?,?,?)";
            pst = con.prepareStatement(query);
            pst.setInt(1, agent_type);
            pst.setInt(2, duration_time);
            pst.setInt(3, count);
            pst.setInt(4, value);
            pst.executeQuery();

        } catch (SQLException e) {
            log.error(e.getMessage());
        }
    }

    public void rules_delete(int id) {
        try {
            String query = "DELETE FROM rules where id=?";
            pst = con.prepareStatement(query);
            pst.setInt(1, id);
            pst.executeQuery();

        } catch (SQLException e) {
            log.error(e.getMessage());
        }
    }

    public void close() {
        try {
            rs.close();
            pst.close();
            con.close();
            log.info("mariaDB disconnect");
        } catch (SQLException e) {
            log.error(e.getMessage());
        }

    }
}
