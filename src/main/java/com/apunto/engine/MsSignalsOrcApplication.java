package com.apunto.engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MsSignalsOrcApplication {

    public static void main(String[] args) {
        SpringApplication.run(MsSignalsOrcApplication.class, args);
    }
}