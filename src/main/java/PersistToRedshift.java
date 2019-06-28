import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

public class PersistToRedshift {
  static final String redshiftUrl = "jdbc:redshift://<cluster-name>.redshift.amazonaws.com:5439/<db-name>";
  static final String masterUsername = "<user-name>";
  static final String password = "<password>";

  public boolean persist(ArrayList<ArrayList<String>> rows) {
    Connection connection = null;
    Statement statement = null;

    try {
      Class.forName("com.amazon.redshift.jdbc42.Driver");
      Properties properties = new Properties();
      properties.setProperty("user", masterUsername);
      properties.setProperty("password", password);
      connection = DriverManager.getConnection(redshiftUrl, properties);
      String sqlStatement = "select region from sales limit 10";
      PreparedStatement ps = connection.prepareStatement(sqlStatement);
      ResultSet rs = ps.executeQuery();
      ArrayList<String> arrayList = new ArrayList<String>(10);
      while(rs.next()){
        String reg = rs.getString("region");
        arrayList.add(reg);
        System.out.print(reg);
      }
      // Further code to follow
    } catch (ClassNotFoundException cnfe) {
      cnfe.printStackTrace();
      return false;
    } catch (SQLException sqle) {
      sqle.printStackTrace();
      return false;
    }
    return true;
  }

}