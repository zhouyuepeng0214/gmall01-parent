package com.atguigu.gmall01.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall01.publisher.mapper")
public class Gmall01PublisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(Gmall01PublisherApplication.class, args);
	}

}
