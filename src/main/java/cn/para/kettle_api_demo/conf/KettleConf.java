package cn.para.kettle_api_demo.conf;


import org.pentaho.di.core.KettleClientEnvironment;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class KettleConf {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @PostConstruct
    public void KettleEnvironmentInit() throws KettleException {
        if (System.getenv("KETTLE_HOME") != null) {
            System.setProperty("DI_HOME", System.getenv("KETTLE_HOME"));
            System.setProperty("KETTLE_HOME", System.getenv("KETTLE_HOME"));
            System.setProperty("org.osjava.sj.root", System.getenv("KETTLE_HOME") + "/simple-jndi");
            logger.info("KETTLE_HOME配置[能自动加载该目录下plugins中的插件]:{}", System.getenv("KETTLE_HOME"));
        }
        if (System.getenv("KETTLE_JNDI_ROOT") != null) {
            System.setProperty("org.osjava.sj.root", System.getenv("KETTLE_JNDI_ROOT"));
            logger.info("Simple-jndi配置根路径:{}", System.getenv("KETTLE_JNDI_ROOT"));
        }
        //初始化kettle环境
        if (!KettleEnvironment.isInitialized()) {
            KettleEnvironment.init();
            KettleClientEnvironment.getInstance().setClient(KettleClientEnvironment.ClientType.SPOON);
            logger.info("kettle 环境初始化完成......");
        }
    }
}