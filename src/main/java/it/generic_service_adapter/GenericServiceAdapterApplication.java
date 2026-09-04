package it.generic_service_adapter;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class GenericServiceAdapterApplication {

  public static void main(String[] args) {
    SpringApplication.run(GenericServiceAdapterApplication.class, args);
  }
}
