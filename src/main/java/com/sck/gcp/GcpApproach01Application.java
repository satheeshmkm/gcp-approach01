package com.sck.gcp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages ={"com.sck"})
public class GcpApproach01Application {

	public static void main(String[] args) {
		SpringApplication.run(GcpApproach01Application.class, args);
	}
}
