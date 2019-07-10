package com.bright.cloudconsumer.controller;


import com.bright.cloudconsumer.constants.CloudRedisKeys;
import com.bright.cloudconsumer.feign.service.InfoClient;
import com.bright.cloudconsumer.redis.RedisManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Set;

@RestController
@Configuration
public class ConsumerController {

    @Autowired
    private InfoClient infoClient;

    @Autowired
    private RedisManager redisManager;

    @RequestMapping(value = "/consumerInfo", method = RequestMethod.GET)
    public String consumerInfo(){
        Set<String> smembers = redisManager.smembers(CloudRedisKeys.BLOG_REAL_IP);
        System.out.println(smembers.toString());
        return infoClient.info();
    }
}
