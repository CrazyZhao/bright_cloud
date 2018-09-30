package com.bright.cloudconsumer.feign.hystrix;

import com.bright.cloudconsumer.feign.service.InfoClient;
import org.springframework.stereotype.Component;

@Component
public class InfoFallBack implements InfoClient {
    @Override
    public String info() {
        return "fallback info";
    }
}
