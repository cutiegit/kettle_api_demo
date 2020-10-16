package cn.para.kettle_api_demo;

import cn.para.kettle_api_demo.etl.DataExtractApplication;
import org.pentaho.di.core.exception.KettleException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KettleApiDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KettleApiDemoApplication.class, args);
        try {
            DataExtractApplication.main(args);
        } catch (KettleException e) {
            e.printStackTrace();
        }
    }

}
