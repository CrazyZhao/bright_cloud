package com.bright.cloudconsumer.feign.service;

import com.bright.cloudconsumer.feign.config.MyFeignConfig;
import com.bright.cloudconsumer.feign.hystrix.InfoFallBack;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.RequestMapping;

//1.name为被调用的服务应用名称
//2.InfoFallBack作为熔断实现，当请求cloud-provider失败时调用其中的方法
//3.feign配置
@FeignClient(name = "cloud-provider", fallback = InfoFallBack.class, configuration = MyFeignConfig.class)
public interface InfoClient {

    //被请求微服务的地址
    @RequestMapping("/info")
    String info();
}
