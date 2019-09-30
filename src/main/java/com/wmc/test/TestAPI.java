package com.wmc.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author: WangMC
 * @date: 2019/9/14 16:44
 * @description:
 */
public class TestAPI {

    public static Configuration conf = null;
    public static Connection connection = null;
    public static Admin admin = null;

    static {
        //HBaseConfiguration的单例方法实例
        try {
            // 1、获取配置信息
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03");
            // 2、创建连接对象
            connection = ConnectionFactory.createConnection(conf);
            // 3、创建Admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        boolean b = admin.tableExists(TableName.valueOf(tableName));
        return b;
    }


    public static void createTable(String tableName, String... columnFamilys) throws IOException {
        //1、判断列族信息是否为空
        if (columnFamilys.length <= 0) {
            System.out.println("请设置列族信息");
            return;
        }

        //2、判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName+"表已存在，请设置表名");
            return;
        }

        //3、创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //4、循环添加列族信息
        for (String columnFamily : columnFamilys) {
            //5、创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            //6、添加具体的列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //创建表
        admin.createTable(hTableDescriptor);

    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }

    public static void dropTable(String tableName) throws IOException {
        //1、判断表是否存在
        if (!isTableExist(tableName)) {
            System.out.println(tableName+"不存在");
            return;
        }
        //2、使表下线
        admin.disableTable(TableName.valueOf(tableName));
        //3、删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 创建命名空间
     * @param nameSpace
     */
    public static void createNameSpace(String nameSpace){

        //1、创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        try {
            //2、创建命名空间
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(nameSpace+"命名空间已存在");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("cunzai!!!!!!");
    }

    /**
     * 向表插入数据
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @param cv
     * @throws IOException
     */
    public static void putData(String tableName,String rowKey,String cf,String cn,String cv) throws IOException {
        //1、获取表的对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2、创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //3、给put对象赋值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(cv));

        //4、插入数据
        table.put(put);

        //5、关闭表连接
        table.close();
    }

    /**
     * get
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @throws IOException
     */
    public static void getData(String tableName,String rowKey,String cf,String cn) throws IOException {

        //1、获取表的对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2、创建get对象
        Get get = new Get(Bytes.toBytes(rowKey));
        //3、给get对象赋值
        //获取指定的列族
        // get.addFamily(Bytes.toBytes(cf));
        //获取指定的列族和列
        // get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        //设置获取数据的版本数
        get.setMaxVersions();

        //4、获取数据
        Result result = table.get(get);
        //5、解析result并打印

        for (Cell cell : result.rawCells()) {
            //6、打印数据
            System.out.println("CF:"+Bytes.toString(CellUtil.cloneFamily(cell))+
                    "CN："+Bytes.toString(CellUtil.cloneQualifier(cell))+
                    "CV："+Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //7、关闭连接
        table.close();
    }

    /**
     * scan
     * @param tableName
     */
    public static void scanData(String tableName) throws IOException {

        //1、获取表连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2、创建scan对象
        Scan scan = new Scan(Bytes.toBytes("1001"),Bytes.toBytes("1002"));

        //3、扫描表
        ResultScanner resultScanner = table.getScanner(scan);

        //4、
        for (Result result : resultScanner) {
            for (Cell cell : result.rawCells()) {
                //6、打印数据
                System.out.println(
                        "RK："+Bytes.toString(CellUtil.cloneRow(cell))+
                        "，CF："+Bytes.toString(CellUtil.cloneFamily(cell))+
                        "，CN："+Bytes.toString(CellUtil.cloneQualifier(cell))+
                        "，CV："+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        // 关闭连接资源
        table.close();
    }


    /**
     * 删除数据
     * @param tableName 表名
     * @param rowKey    行键
     * @param cf        列簇
     * @param cn        列名
     */
    public static void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {
        //1、获取表连接
        Table table = connection.getTable(TableName.valueOf(tableName));
        //2、创建delete对象
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //3、设置删除的列
        // delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn));
        delete.addColumns(Bytes.toBytes(cf),Bytes.toBytes(cn));

        //4、执行删除操作
        table.delete(delete);
        //5、关闭连接
        table.close();
    }

    public static void main(String[] args) throws IOException {

        //1、测试表是否存在
        // System.out.println(isTableExist("stu3"));
        //2、创建表测试
        // createTable("test:stu3","info1","info2");
        //3、删除表测试
        // dropTable("stu3");
        // System.out.println(isTableExist("test:stu3"));

        //4、创建命名空间测试
        // createNameSpace("test");

        //5、插入数据测试
        // putData("stu","1004","info","name","test");

        //6、获取资源测试
        // getData("stu", "1001", "info", "name");


        //7、扫描表
        // scanData("test:stu3");

        //8、删除行数据
        deleteData("stu","1007","info","name" );
        //关闭资源
        close();
    }
}
