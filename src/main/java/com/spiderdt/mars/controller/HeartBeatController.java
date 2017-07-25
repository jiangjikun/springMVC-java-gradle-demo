package com.spiderdt.mars.controller;

import com.spiderdt.mars.util.Slog;
import com.spiderdt.mars.util.Sredis;
import com.wordnik.swagger.annotations.ApiOperation;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
class HeartBeatController {

    @Autowired
    Slog slog;
    @Autowired
    Sredis sredis;
    @RequestMapping(value = "/heartbeat", method = RequestMethod.GET)
    @ApiOperation(value = "/heartbeat", notes = "心跳测试 getOverallScore")
    @ResponseBody
    public ResponseEntity<String> getOverallScore() {
        JSONObject response = new JSONObject();
        slog.info("this is heartbeat");
        String headerbeat_redis_key = "test_heartbeat";
        String redis_ret = sredis.getString(headerbeat_redis_key);
        String ret = null;
        if(null != redis_ret ){
            //判断字符串是否是json格式。
            ret = "";//format to json
            slog.info(" get from redis data is oK");
        }else{
            slog.info(" get from redis data empty go on");
            //原来逻辑
            sredis.addString(headerbeat_redis_key,ret);
        }
        //return json
        sredis.addString("test_heartbeat","OK");
        slog.info("redis return:"+sredis.getString("test_heartbeat"));
        slog.debug("test");
        slog.error("test");
        response.put("status", "success");
        return ResponseEntity.status(HttpStatus.OK).body(response.toString());
    }
}
