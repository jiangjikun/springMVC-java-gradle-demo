package com.spiderdt.mars.dao;

import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;


@Repository
public interface CommonDao {


    String lastDate(@Param("xtable") String table);


}
