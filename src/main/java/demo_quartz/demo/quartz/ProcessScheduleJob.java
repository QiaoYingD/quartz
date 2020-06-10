package demo_quartz.demo.quartz;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;

import demo_quartz.demo.entity.Schedule;
import demo_quartz.demo.util.Constants;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;
import java.util.Date;

public class ProcessScheduleJob implements Job {

    //模拟数据库
    private static Map<Integer, Schedule> scheduleMap = new HashMap<>(6);

    static {
        Schedule schedule1 = new Schedule();
        schedule1.setId(1);
        schedule1.setCrontab("1");
        scheduleMap.put(1, schedule1);
        Schedule schedule2 = new Schedule();
        schedule2.setId(2);
        scheduleMap.put(2, schedule2);
        Schedule schedule3 = new Schedule();
        schedule3.setId(3);
        scheduleMap.put(3, schedule3);
        Schedule schedule4 = new Schedule();
        schedule4.setId(4);
        scheduleMap.put(4, schedule4);
    }


    private static final Logger logger = LoggerFactory.getLogger(ProcessScheduleJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {

        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();

        int projectId = jobDataMap.getInt(Constants.PROJECT_ID);
        int scheduleId = jobDataMap.getInt(Constants.SCHEDULE_ID);


        Date scheduledFireTime = context.getScheduledFireTime();

        Date fireTime = context.getFireTime();

        logger.info("scheduled fire time :{}, fire time :{}, process id :{}", scheduledFireTime, fireTime, scheduleId);

        // query schedule 查询数据库定时任务是否存在，不存在则删除，自行模拟查询数据库
        Schedule schedule = scheduleMap.get(scheduleId);
        if (schedule == null) {
            logger.warn("process schedule does not exist in db，delete schedule job in quartz, projectId:{}, scheduleId:{}", projectId, scheduleId);
            deleteJob(projectId, scheduleId);
            return;
        }


        logger.info("hello jon执行时间: " + LocalTime.now());
        System.out.println("hello jon执行时间: " + LocalTime.now());
    }


    /**
     * delete job
     */
    private void deleteJob(int projectId, int scheduleId) {
        String jobName = QuartzExecutors.buildJobName(scheduleId);
        String jobGroupName = QuartzExecutors.buildJobGroupName(projectId);
        QuartzExecutors.getInstance().deleteJob(jobName, jobGroupName);
    }
}
