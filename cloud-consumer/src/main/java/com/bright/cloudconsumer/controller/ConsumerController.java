package com.bright.cloudconsumer.controller;


import com.bright.cloudconsumer.feign.service.InfoClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Configuration
public class ConsumerController {

    @Autowired
    InfoClient infoClient;

    @RequestMapping(value = "/consumerInfo", method = RequestMethod.GET)
    public String consumerInfo(){
        return infoClient.info();
    }
}
