package demo_quartz.demo.config;

import demo_quartz.demo.quartz.QuartzExecutors;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
public class QuartzConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(QuartzConfiguration.class);

    @PostConstruct
    public void run() {
        try {
            // 项目启动加载Quartz定时器
            QuartzExecutors.getInstance().start();
        } catch (SchedulerException e) {
            try {
                //当加载Quartz定时器异常时，停止Quartz定时器
                QuartzExecutors.getInstance().shutdown();
            } catch (SchedulerException e1) {
                logger.error("QuartzExecutors shutdown failed : " + e1.getMessage(), e1);
            }
            logger.error("start Quartz failed : " + e.getMessage(), e);
        }
    }


}
