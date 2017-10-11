package lg.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * Created by Administrator on 2016/3/16.
 */
public class PoolConnection {
    private static PoolConnection poolConnection;
    private LinkedList<Connection> connectionList  = new LinkedList<Connection>();
    //初始化连接池容量
    private final static int initSize = 1;
    //连接池最大容量
    private final static int maxCount = Integer.valueOf(Parameters.MaxConnectionNum);
    //当前连接池分配出去的连接数
    private int currentAllocateCount  = 0;
    //设定当前连接池容量
    private int poolSize=initSize;
    //数据库URl连接
    private final  static String url =Parameters.DataBaseURL;
    //数据库用户名
    private final  static String user  =Parameters.DataBaseUserName;
    //数据库密码
    private final  static String password  =Parameters.DataBaseUserPassword;
    // singleton pattern(懒汉单例模式)
    public static   synchronized PoolConnection getPoolConnection()
    {
        if(poolConnection==null)
        {
            poolConnection = new PoolConnection();
        }
        return poolConnection;
    }
    //在初始化的时候初始化一定数量的数据库连接
    private PoolConnection()
    {
        try {
            Class.forName(Parameters.JDBCDriverString);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            throw new ExceptionInInitializerError(e);
        }
        for(int i=0;i<initSize;i++)
        {
            //添加数据库连接到list 尾部
            connectionList.addLast(initConnection());
        }
    }
    //私有方法，提供创建数据库连接的实现
    private Connection initConnection()
    {
        Connection connection  =null;
        try
        {  //这里面调用的是一个实现了Connection 的代理类，代理真实的Connection对象
            connection=DriverManager.getConnection(url, user, password);
        }catch(SQLException ex)
        {
            throw new ExceptionInInitializerError(ex);
        }
        return connection;

    }
    //从连接池里面取出一个数据库连接
    public synchronized  Connection getConnection()
    {
        //如果分发出去的数量等于 容器所允许的最大值，就让他休息一会儿,这块儿功能还没有真正的实现,在并发的时候出现错误
        //如果已经分配给程序使用的数据库连接数大于规定的最大连接数，就等待其他线程把相应的连接放回连接池之后再进行下一步操作
        if(currentAllocateCount>=maxCount)
        {
            try {
                wait(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            getConnection();
        }
        //如果已经分配给程序使用的数据库连接数等于连接池容量
        if(currentAllocateCount ==poolSize)
        {
            //添加数据库连接到整个List尾部
            connectionList.addLast(initConnection());
            //容量++
            poolSize ++ ;
        }
        //已经分配给程序使用的数据库连接数 ++
        currentAllocateCount++;
        //返回并且移除List第一个数据库连接(Connection)
        return connectionList.removeFirst();
    }
    //把相应的数据库资源放回连接池
    public synchronized void freeConnection(Connection connection)
    {
        //已经分配给程序使用的数据库连接数 --
        if(connection!=null)
            currentAllocateCount--;
        //添加数据库连接到整个List尾部
        connectionList.addLast(connection);
    }

}
