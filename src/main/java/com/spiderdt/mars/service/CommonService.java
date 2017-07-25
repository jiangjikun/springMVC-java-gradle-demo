package com.spiderdt.mars.service;

import com.spiderdt.mars.dao.CommonDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by kun on 2017/4/6.
 */
@Service
class CommonService {
    @Autowired
    CommonDao commonDao;

    public String lastDate() {
        return commonDao.lastDate("model.d_bolome_orders");
    }

}
