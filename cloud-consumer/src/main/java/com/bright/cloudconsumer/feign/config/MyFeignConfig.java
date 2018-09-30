package com.bright.cloudconsumer.feign.config;

import feign.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MyFeignConfig {

    /**
     * feign打印日志等级
     * @return
     */
    @Bean
    Logger.Level feignLoggerLeval(){
        return Logger.Level.FULL;
    }
}
