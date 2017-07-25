package com.spiderdt.mars.service;

import com.spiderdt.mars.util.Slog;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "/spring/applicationContext.xml")
@WebAppConfiguration
public class CommonTest extends TestCase {

    @Autowired
    CommonService commonService;

    @Autowired
    Slog slog;

    @Test
    public void lastDateTest() {
        slog.info(commonService.lastDate());
    }
}
