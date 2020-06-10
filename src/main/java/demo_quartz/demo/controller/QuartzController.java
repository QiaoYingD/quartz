package demo_quartz.demo.controller;

import demo_quartz.demo.entity.Schedule;
import demo_quartz.demo.quartz.ProcessScheduleJob;
import demo_quartz.demo.quartz.QuartzExecutors;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Map;

@RestController
@RequestMapping("/quartz")
public class QuartzController {


    @GetMapping("/addCronJob")
    public void addCronJob(Integer projectId, Integer scheduleId, String cronExpression) {

        String jobName = QuartzExecutors.buildJobName(scheduleId);
        String jobGroupName = QuartzExecutors.buildJobGroupName(projectId);


        Map<String, Object> dataMap = QuartzExecutors.buildDataMap(projectId, scheduleId);

        QuartzExecutors.getInstance().addCronJob(ProcessScheduleJob.class, jobName, jobGroupName, cronExpression, dataMap);

    }


    @GetMapping("/addSimpleJob")
    public void addSimpleJob(Integer projectId, Integer scheduleId, String simpleExpression, String unitType) {

        String jobName = QuartzExecutors.buildJobName(scheduleId);
        String jobGroupName = QuartzExecutors.buildJobGroupName(projectId);

        Map<String, Object> dataMap = QuartzExecutors.buildDataMap(projectId, scheduleId);

        QuartzExecutors.getInstance().addSimpleJob(ProcessScheduleJob.class, jobName, jobGroupName, simpleExpression, dataMap, unitType);

    }


    @GetMapping("/deleteJob")
    public void deleteJob(Integer projectId, Integer scheduleId) {

        String jobName = QuartzExecutors.buildJobName(scheduleId);
        String jobGroupName = QuartzExecutors.buildJobGroupName(projectId);

        QuartzExecutors.getInstance().deleteJob(jobName, jobGroupName);

    }

}
