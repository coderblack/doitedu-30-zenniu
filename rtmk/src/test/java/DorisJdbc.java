import java.sql.*;

public class DorisJdbc {
    public static void main(String[] args) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu:9030","root","");

        Statement statement = conn.createStatement();

        ResultSet resultSet = statement.executeQuery("select * from rtmk.dwd_mall_events");

        while(resultSet.next()){
            System.out.println(resultSet.getString("eventId"));
        }

        statement.close();
        conn.close();
    }
}
