package com.test.util;

import com.test.entity.UserInfo;

import java.io.IOException;
import java.util.List;

public interface HBaseInterface<T> {

    void insertList(String tableName, List<T> objectList) throws IOException;

}
