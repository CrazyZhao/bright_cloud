package com.bright.cloudprovider;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class CloudProviderApplication {


    /*@RequestMapping("/")
    public String hello() {
        return "Hello World: CloudProvider!";
    }*/


    public static void main(String[] args) {
        SpringApplication.run(CloudProviderApplication.class, args);
    }
}
