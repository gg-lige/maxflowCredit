package lg.java;
/**
 * 注意版本问题，选择java 8--》在project structure中修改
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by lg on 2017/6/19.
 * 数据是否读取成功。
 */
public class Parameters {

    public static final String DataBaseURL;
    public static final String DataBaseUserName;
    public static final String DataBaseUserPassword;
    public static final String JDBCDriverString;
    public static final String MaxConnectionNum;
    public static final String ThreadNum;

    static{
        Properties properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("db.properties"));
        } catch (FileNotFoundException ex){
            System.out.println("读取属性文件--->失败！- 原因：文件路径错误或者文件不存在");
            System.out.println("需要将para.proerties文件所在目录加入到classpath中！");
            ex.printStackTrace();
        } catch (IOException ex){
            System.out.println("装载文件--->失败!");
            ex.printStackTrace();
        }
        assert(properties.containsKey("DataBaseURL"));
        DataBaseURL= properties.getProperty("DataBaseURL");

        assert(properties.containsKey("MaxConnectionNum"));
        MaxConnectionNum= properties.getProperty("MaxConnectionNum");

       assert(properties.containsKey("DataBaseUserName"));
        DataBaseUserName= properties.getProperty("DataBaseUserName");

        assert(properties.containsKey("ThreadNum"));
        ThreadNum= properties.getProperty("ThreadNum");

        assert(properties.containsKey("DataBaseUserPassword"));
        DataBaseUserPassword= properties.getProperty("DataBaseUserPassword");

        assert(properties.containsKey("JDBCDriverString"));
        JDBCDriverString= properties.getProperty("JDBCDriverString");
    }

}
