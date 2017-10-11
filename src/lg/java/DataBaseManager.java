package lg.java;



import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import java.sql.DriverManager;

public class DataBaseManager {

	/*
	private static String url = "jdbc:oracle:thin:@10.199.132.7:1521:nsxygl";
	private static String user = "sjck_zzs";
	private static String password = "sjck_zzs";
	*/

	/*
	static {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	*/

    private DataBaseManager() {}

    public static void execute(String sql){
        Connection connection = null;
        Statement Statement = null;
        try {
            //connection = DriverManager.getConnection(url, user, password);
            connection = PoolConnection.getPoolConnection().getConnection();
            Statement = connection.createStatement();
            Statement.execute(sql);
        }catch (Exception e){
            e.printStackTrace();

        }finally {
            try {
                if (Statement != null) {
                    Statement.close();
                }
                if (connection != null) {
                    //connection.close();
                    PoolConnection.getPoolConnection().freeConnection(connection);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static void update(String sql, Object... objects) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            //connection = DriverManager.getConnection(url, user, password);
            connection = PoolConnection.getPoolConnection().getConnection();
            preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < objects.length; i++) {
                if(objects[i]==null){
                    preparedStatement.setObject(i + 1, "");
                }else {
                    preparedStatement.setObject(i + 1, objects[i]);
                }
            }
            preparedStatement.execute();
        }catch (Exception e){
            e.printStackTrace();

        }finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    //connection.close();
                    PoolConnection.getPoolConnection().freeConnection(connection);
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public static List<Map<String, Object>> query(String sql, Object... objects) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        try {
            //connection = DriverManager.getConnection(url, user, password);
            connection = PoolConnection.getPoolConnection().getConnection();
            preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < objects.length; i++) {
                preparedStatement.setObject(i + 1, objects[i]);
            }
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (int i = 0; i < resultSet.getMetaData().getColumnCount(); i++) {
                    map.put(resultSet.getMetaData().getColumnName(i + 1), resultSet.getObject(i + 1));
                }
                list.add(map);
            }

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    //connection.close();
                    PoolConnection.getPoolConnection().freeConnection(connection);
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return list;
    }
}