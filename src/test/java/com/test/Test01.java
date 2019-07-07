package com.test;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.test.entity.UserInfo;
import com.test.pojo.HBaseBean;
import com.test.util.HBaseUtil;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest
public class Test01<T> {

    @Autowired
    private HbaseTemplate hbaseTemplate;

    @Autowired
    private HBaseAdmin hBaseAdmin;

    /**
     * 创建表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        HTableDescriptor desc=new HTableDescriptor(TableName.valueOf("Student"));
        desc.addFamily(new HColumnDescriptor("info".getBytes()));
        if(hBaseAdmin.tableExists("Student")){
            System.err.println("===========================table====exists====================================");
        }
        hBaseAdmin.createTable(desc);
    }


    /**
     * 添加数据
     */
    @Test
    public void insert()  {
        hbaseTemplate.put("Student","row5","info","dajitui","111".getBytes());
        hbaseTemplate.put("Student","row2","info","score","99".getBytes());
        hbaseTemplate.put("Student","row3","info","name","和凯凯".getBytes());
    }



    /**
     * 查询所有数据
     */
    @Test
    public void findAll(){
        Object obj= hbaseTemplate.find("Student",new Scan(),(Result result, int i)->{
            Cell[] cells = result.rawCells();
            List<HBaseBean> list=new ArrayList<>();
                for (Cell cell : cells) {
                   String columnFamily= new String(CellUtil.cloneFamily(cell));
                    String rowName = new String(CellUtil.cloneRow(cell));
                    String key = new String(CellUtil.cloneQualifier(cell ));
                    String value = new String(CellUtil.cloneValue(cell));
                    list.add(new HBaseBean(columnFamily,rowName,key,value));
                }
                return list;
        });
        String json = JSONArray.toJSONString(obj);
        System.out.println(json);
    }

    @Test
    public void findOntColumn(){
         Object result=hbaseTemplate.get("Student", "row1",new RowMapper<Map<String,Object>>(){
            @Override
            public Map<String, Object> mapRow(Result result, int i) throws Exception {
                List<Cell> ceList =   result.listCells();
                Map<String,Object> map = new HashMap<String, Object>();
                if(ceList!=null&&ceList.size()>0){
                    for(Cell cell:ceList){
                        String key = new String(CellUtil.cloneQualifier(cell ));
                        String value = new String(CellUtil.cloneValue(cell));
                        map.put(key,value);
                    }
                }
                return map;
            }
        });
        System.out.println(JSON.toJSONString(result));
    }

    @Resource
    HBaseUtil hBaseUtil;

    @Test
    public void  getAll(){
        hBaseUtil.searchAll();
        //hbaseTemplate.delete("Student","100001001010100101001101010101011000100000000000000000000000","info");
    }


    /**
     * 添加列,族(family)
     * @throws IOException
     */
    @Test
    public void addFamily() throws IOException {
        TableName tableName = TableName.valueOf("Student");
        hBaseAdmin.disableTable(tableName);
        HTableDescriptor desc = hBaseAdmin.getTableDescriptor(tableName);
        HColumnDescriptor column=new HColumnDescriptor("abc");
        desc.addFamily(column);
        hBaseAdmin.modifyTable("Student",desc);
        hBaseAdmin.enableTable(tableName);
        hBaseAdmin.close();
    }


    @Test
    public void a(){
        List<UserInfo> list=new ArrayList<>();
        UserInfo userInfo=new UserInfo();
        userInfo.setName("eee");
        userInfo.setUid(2);
        list.add(userInfo);
        try {
            new HBaseUtil().insertList("Student",list);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
