package com.test.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.test.entity.UserInfo;
import com.test.pojo.HBaseBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * hbase工具类
 */
@Component
public class HBaseUtil implements HBaseInterface, InitializingBean {

    private static SnowflakeIdWorker snowflakeIdWorker = new SnowflakeIdWorker(0, 0);

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Autowired
    private HBaseAdmin hBaseAdmin;

    private static Connection conn;

    //zookeeper集群
    private static String zookeeper = "xx";

    public static void init() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zookeeper);
        try {
            conn = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建一个表,一个列(族)
     *
     * @throws IOException
     */
    public void createTable(String tableName, String family) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
        desc.addFamily(new HColumnDescriptor(family.getBytes()));
        if (hBaseAdmin.tableExists(tableName)) {

            System.err.println("===========================table====exists====================================");
            throw new RuntimeException("该表已存在!!");
        }
        hBaseAdmin.createTable(desc);
        hBaseAdmin.close();
    }


    /**
     * 给 指定表 添加列,族(family)
     * 可以添加多个
     *
     * @throws IOException
     */
    public void addFamily(String tablename, String[] familyName) throws IOException {
        TableName tableName = TableName.valueOf(tablename);
        hBaseAdmin.disableTable(tableName);
        HTableDescriptor desc = hBaseAdmin.getTableDescriptor(tableName);
        for (int i = 0; i < familyName.length; i++) {
            HColumnDescriptor column = new HColumnDescriptor(familyName[i]);
            desc.addFamily(column);
        }
        hBaseAdmin.modifyTable(tablename, desc);
        hBaseAdmin.enableTable(tableName);
        hBaseAdmin.close();
    }

    /**
     * 给 指定表,指定行,指定列,指定qualifier 添加指定数据
     */
    public void insert(String tableName, String rowName, String familyName, String qualifter, String value) {
        hbaseTemplate.put(tableName, rowName, familyName, qualifter, value.getBytes());
    }


    /**
     * 查看 表中所有数据,以json形式输出
     *
     * @param tableName
     * @return
     */
    public String findAll(String tableName) {
        Object obj = hbaseTemplate.find(tableName, new Scan(), (Result result, int i) -> {
            Cell[] cells = result.rawCells();
            List<HBaseBean> list = new ArrayList<>();
            for (Cell cell : cells) {
                String columnFamily = new String(CellUtil.cloneFamily(cell));
                String rowName = new String(CellUtil.cloneRow(cell));
                String key = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                list.add(new HBaseBean(columnFamily, rowName, key, value));
            }
            return list;
        });
        String json = JSONArray.toJSONString(obj);
        return json;
    }


    @Override
    public void insertList(String tableName, List objectList) throws IOException {
        if (!CollectionUtils.isEmpty(objectList)) {

            Table table = conn.getTable(TableName.valueOf(tableName));
            Iterator iterator = objectList.iterator();
            while (iterator.hasNext()) {
                Object object = iterator.next();
                try {
                    Class<?> class1 = Class.forName(object.getClass().getName());
                    Field[] fields = class1.getDeclaredFields();
                    List<Put> puts = new ArrayList<>(fields.length);
                    for (Field field : fields) {
                        //打开私有访问
                        field.setAccessible(true);
                        //获取属性
                        String name = field.getName();
                        //获取属性值
                        Object value = field.get(object);
                        if (value != null) {
                            Put put1 = new Put(Bytes.toBytes(Long.toBinaryString(snowflakeIdWorker.nextId())));
                            put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("yy"), JSON.toJSONString(name).getBytes());
                            put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("yy"), JSON.toJSONString(value).getBytes());
                            puts.add(put1);
                        }
                    }
                    if (!CollectionUtils.isEmpty(puts)) {
                        table.put(puts);
                    }
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public void searchAll() {
        Object object = hbaseTemplate.find("Student", new Scan(), (Result result, int i) -> {
            UserInfo userInfo=new UserInfo();
            BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(userInfo);
            List<Cell> ceList = result.listCells();
            for (Cell cellItem : ceList) {
                String cellName = new String(CellUtil.cloneQualifier(cellItem));
                beanWrapper.setPropertyValue(cellName, new String(CellUtil.cloneValue(cellItem)));
            }
            return userInfo;
        });
        System.out.println(object);
    }


    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }
}
