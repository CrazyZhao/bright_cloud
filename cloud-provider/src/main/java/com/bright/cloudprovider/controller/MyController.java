package com.bright.cloudprovider.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class MyController {

    @RequestMapping(value = "/info", method = RequestMethod.GET)
    public String info() {
        try {
            //休眠2秒，测试超时服务熔断[直接关闭服务提供者亦可]
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "Hello, cloud-provider";
    }
}
