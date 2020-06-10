/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package demo_quartz.demo.quartz;


import demo_quartz.demo.entity.Schedule;
import demo_quartz.demo.util.Constants;
import org.apache.commons.lang3.StringUtils;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * single Quartz executors instance
 */
public class QuartzExecutors {

    /**
     * logger ofQuartzExecuors
     */
    private static final Logger logger = LoggerFactory.getLogger(QuartzExecutors.class);


    /**
     * read write lock
     */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * A Scheduler maintains a registry of org.quartz.JobDetail and Trigger.
     */
    private Scheduler scheduler;


    private QuartzExecutors() {
    }


    /**
     * instance of QuartzExecutors
     */
    private static volatile QuartzExecutors INSTANCE = null;

    /**
     * thread safe and performance promote
     * 线程安全与性能提升
     * <p>
     * 单例的双重锁模式
     *
     * @return instance of Quartz Executors
     */
    public static QuartzExecutors getInstance() {
        if (INSTANCE == null) {
            synchronized (QuartzExecutors.class) {
                if (INSTANCE == null) {
                    // when more than two threads run into the first null check same time, to avoid instanced more than one time, it needs to be checked again.
                    //当两个以上的线程同时运行第一个空检查时，为了避免多次实例化，需要再次检查
                    INSTANCE = new QuartzExecutors();
                    //finish QuartzExecutors init
                    //完成 QuartzExecutors 实例
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * init
     * <p>
     * Returns a client-usable handle to a Scheduler.
     */
    private void init() {

        try {
            SchedulerFactory schedulerFactory = new StdSchedulerFactory(Constants.QUARTZ_PROPERTIES_PATH);
            scheduler = schedulerFactory.getScheduler();
        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
            System.exit(1);
        }
    }


    /**
     * Whether the scheduler has been started.
     * 判断程序是否已启动,未启动则启动
     *
     * @throws SchedulerException scheduler exception
     */
    public void start() throws SchedulerException {
        if (!scheduler.isStarted()) {
            scheduler.start();
            logger.info("Quartz service started");
        }
    }

    /**
     * stop all scheduled tasks
     * <p>
     * Halts the Scheduler's firing of Triggers,
     * and cleans up all resources associated with the Scheduler.
     * <p>
     * 停止调度程序触发，并清除与调度程序关联的所有资源
     *
     * <p>
     * The scheduler cannot be re-started.
     *
     * @throws SchedulerException scheduler exception
     */
    public void shutdown() throws SchedulerException {
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
            logger.info("Quartz service stopped, and halt all tasks");
        }
    }

    /**
     * add task cornTrigger , if this task already exists, return this task with updated trigger
     *
     * @param clazz          job class name
     * @param jobName        job name
     * @param jobGroupName   job group name
     * @param cronExpression cron expression
     * @param jobDataMap     job parameters data map
     */
    public void addCronJob(Class<? extends Job> clazz, String jobName, String jobGroupName,
                           String cronExpression, Map<String, Object> jobDataMap) {
        //写锁加锁
        lock.writeLock().lock();
        try {
            /**
             *  JobDetail：任务的实现类以及类的信息
             */
            JobKey jobKey = new JobKey(jobName, jobGroupName);
            JobDetail jobDetail;
            //add a task (if this task already exists, return this task directly)
            //添加任务（如果此任务已存在，则直接返回此任务）
            if (scheduler.checkExists(jobKey)) {
                jobDetail = scheduler.getJobDetail(jobKey);

                if (jobDataMap != null) {
                    jobDetail.getJobDataMap().putAll(jobDataMap);
                }

            } else {
                jobDetail = JobBuilder.newJob(clazz).withIdentity(jobKey).build();

                if (jobDataMap != null) {
                    jobDetail.getJobDataMap().putAll(jobDataMap);
                }

                scheduler.addJob(jobDetail, false, true);

                logger.info("Add job, job name: {}, group name: {}",
                        jobName, jobGroupName);
            }


            /**
             * 触发器 :   CronTrigger 实现更复杂的业务逻辑，比如每周五执行一次就用到了CronTrigger，实际上CronTrigger比SimpleTrigger更常用
             *              simpleTrigger 定频率的执行某一个任务
             */
            TriggerKey triggerKey = new TriggerKey(jobName, jobGroupName);

            /**
             *
             * Instructs the Scheduler that upon a mis-fire
             * situation, the CronTrigger wants to have it's
             * next-fire-time updated to the next time in the schedule after the
             * current time (taking into account any associated Calendar),
             * but it does not want to be fired now.
             *
             */

            CronTrigger cronTrigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression).withMisfireHandlingInstructionDoNothing())
                    .forJob(jobDetail).build();

            //当触发器存在时，进行修改触发器，不存在时直接创建触发器
            if (scheduler.checkExists(triggerKey)) {
                // updateProcessInstance scheduler trigger when scheduler cycle changes
                // 当计划程序周期更改时，updateProcessInstance计划程序触发器
                CronTrigger oldCronTrigger = (CronTrigger) scheduler.getTrigger(triggerKey);
                String oldCronExpression = oldCronTrigger.getCronExpression();
                //当前cron表达式和老的表达式不相同时，重新安排触发器
                if (!StringUtils.equalsIgnoreCase(cronExpression, oldCronExpression)) {

                    /**
                     *  scheduler 调度器
                     *
                     *  定时定频率的执行JobDetail的信息并且通过他将jobDetail和trigger绑定在一起，然后通过他来调用这些方法来执行业务
                     *
                     *  reschedule job trigger
                     */
                    scheduler.rescheduleJob(triggerKey, cronTrigger);
                    logger.info("reschedule job trigger, triggerName: {}, triggerGroupName: {}, cronExpression: {}",
                            jobName, jobGroupName, cronExpression);
                }
            } else {

                /**
                 *  scheduler 调度器
                 *
                 *  定时定频率的执行JobDetail的信息并且通过他将jobDetail和trigger绑定在一起，然后通过他来调用这些方法来执行业务
                 */
                scheduler.scheduleJob(cronTrigger);
                logger.info("schedule job trigger, triggerName: {}, triggerGroupName: {}, cronExpression: {}",
                        jobName, jobGroupName, cronExpression);
            }


        } catch (Exception e) {
            logger.error("add cronJob failed", e);
            throw new RuntimeException("add cronJob failed:" + e.getMessage());
        } finally {
            //最后必须释放锁
            lock.writeLock().unlock();
        }
    }


    /**
     * add task simpleTrigger , if this task already exists, return this task with updated trigger
     *
     * @param clazz            job class name
     * @param jobName          job name
     * @param jobGroupName     job group name
     * @param simpleExpression simple expression
     * @param jobDataMap       job parameters data map
     */
    public void addSimpleJob(Class<? extends Job> clazz, String jobName, String jobGroupName,
                             String simpleExpression, Map<String, Object> jobDataMap, String unitType) {
        //写锁加锁
        lock.writeLock().lock();
        try {
            /**
             *  JobDetail：任务的实现类以及类的信息
             */
            JobKey jobKey = new JobKey(jobName, jobGroupName);
            JobDetail jobDetail;
            //add a task (if this task already exists, return this task directly)
            //添加任务（如果此任务已存在，则直接返回此任务）
            if (scheduler.checkExists(jobKey)) {
                jobDetail = scheduler.getJobDetail(jobKey);

                if (jobDataMap != null) {
                    jobDetail.getJobDataMap().putAll(jobDataMap);
                }

            } else {
                jobDetail = JobBuilder.newJob(clazz).withIdentity(jobKey).build();
                if (jobDataMap != null) {
                    jobDetail.getJobDataMap().putAll(jobDataMap);
                }
                scheduler.addJob(jobDetail, false, true);
                logger.info("Add job, job name: {}, group name: {}",
                        jobName, jobGroupName);
            }

            /**
             * 触发器 :   CronTrigger 实现更复杂的业务逻辑，比如每周五执行一次就用到了CronTrigger，实际上CronTrigger比SimpleTrigger更常用
             *              simpleTrigger 定频率的执行某一个任务
             */
            TriggerKey triggerKey = new TriggerKey(jobName, jobGroupName);

            int simpleNum = Integer.parseInt(simpleExpression);
            SimpleScheduleBuilder simpleScheduleBuilder = null;
            switch (unitType) {
                //秒
                case "0":
                    simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInSeconds(simpleNum).repeatForever();
                    break;
                //分
                case "1":
                    simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInMinutes(simpleNum).repeatForever();
                    break;
                //时
                case "2":
                    simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInHours(simpleNum).repeatForever();
                    break;
                //天
                default:
                    simpleScheduleBuilder = SimpleScheduleBuilder.simpleSchedule().withIntervalInHours(simpleNum * 24).repeatForever();
            }


            SimpleTrigger simpleTrigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
                    .withSchedule(simpleScheduleBuilder).forJob(jobDetail).build();

            //当触发器存在时，进行修改触发器，不存在时直接创建触发器
            if (scheduler.checkExists(triggerKey)) {
                // updateProcessInstance scheduler trigger when scheduler cycle changes
                // 当计划程序周期更改时，updateProcessInstance计划程序触发器
                SimpleTrigger oldSimpleTrigger = (SimpleTrigger) scheduler.getTrigger(triggerKey);
                int oldSimpleNum = oldSimpleTrigger.getRepeatCount();
                //当前cron时间间隔数和老的时间间隔数不相同时，重新安排触发器
                if (simpleNum != oldSimpleNum) {
                    /**
                     *  scheduler 调度器
                     *
                     *  定时定频率的执行JobDetail的信息并且通过他将jobDetail和trigger绑定在一起，然后通过他来调用这些方法来执行业务
                     *
                     *  reschedule job trigger
                     */
                    scheduler.rescheduleJob(triggerKey, simpleTrigger);

                    logger.info("reschedule job trigger, triggerName: {}, triggerGroupName: {}, simpleExpression: {}, startDate: {}, endDate: {}",
                            jobName, jobGroupName, simpleExpression);
                }
            }else {
                /**
                 *  scheduler 调度器
                 *
                 *  定时定频率的执行JobDetail的信息并且通过他将jobDetail和trigger绑定在一起，然后通过他来调用这些方法来执行业务
                 *
                 *  reschedule job trigger
                 */
                scheduler.scheduleJob(simpleTrigger);

                logger.info("reschedule job trigger, triggerName: {}, triggerGroupName: {}, simpleExpression: {}, startDate: {}, endDate: {}",
                        jobName, jobGroupName, simpleExpression);
            }

        } catch (Exception e) {
            logger.error("add simpleJob failed", e);
            throw new RuntimeException("add simpleJob failed:" + e.getMessage());
        } finally {
            //最后必须释放锁
            lock.writeLock().unlock();
        }


    }

    /**
     * delete job
     *
     * @param jobName      job name
     * @param jobGroupName job group name
     * @return true if the Job was found and deleted.
     */
    public boolean deleteJob(String jobName, String jobGroupName) {
        lock.writeLock().lock();
        try {
            JobKey jobKey = new JobKey(jobName, jobGroupName);
            if (scheduler.checkExists(jobKey)) {
                logger.info("try to delete job, job name: {}, job group name: {},", jobName, jobGroupName);
                return scheduler.deleteJob(jobKey);
            } else {
                return true;
            }
        } catch (Exception e) {
            logger.error(String.format("delete job : %s failed", jobName), e);
        } finally {
            lock.writeLock().unlock();
        }
        return false;
    }

    /**
     * build job name
     *
     * @param scheduleId process id
     * @return job name
     */
    public static String buildJobName(int scheduleId) {
        StringBuilder sb = new StringBuilder(30);
        sb.append(Constants.QUARTZ_JOB_PRIFIX).append(Constants.UNDERLINE).append(scheduleId);
        return sb.toString();
    }

    /**
     * build job group name
     *
     * @param projectId project id
     * @return job group name
     */
    public static String buildJobGroupName(int projectId) {
        StringBuilder sb = new StringBuilder(30);
        sb.append(Constants.QUARTZ_JOB_GROUP_PRIFIX).append(Constants.UNDERLINE).append(projectId);
        return sb.toString();
    }

    /**
     * add params to map
     *
     * @param projectId  project id
     * @param scheduleId schedule id
     * @param schedule   schedule
     * @return data map
     */
    public static Map<String, Object> buildDataMap(int projectId, int scheduleId, Schedule schedule) {
        Map<String, Object> dataMap = new HashMap<>(3);
        dataMap.put(Constants.PROJECT_ID, projectId);
        dataMap.put(Constants.SCHEDULE_ID, scheduleId);
        dataMap.put(Constants.SCHEDULE, schedule);
        return dataMap;
    }

    /**
     * add params to map
     *
     * @param projectId  project id
     * @param scheduleId schedule id
     * @return data map
     */
    public static Map<String, Object> buildDataMap(int projectId, int scheduleId) {
        Map<String, Object> dataMap = new HashMap<>(3);
        dataMap.put(Constants.PROJECT_ID, projectId);
        dataMap.put(Constants.SCHEDULE_ID, scheduleId);
        return dataMap;
    }


    /**
     * add task trigger , if this task already exists, return this task with updated trigger
     *
     * @param clazz          job class name
     * @param jobName        job name
     * @param jobGroupName   job group name
     * @param startDate      job start date
     * @param endDate        job end date
     * @param cronExpression cron expression
     * @param jobDataMap     job parameters data map
     */
//    public void addJob(Class<? extends Job> clazz, String jobName, String jobGroupName, Date startDate, Date endDate,
//                       String cronExpression, Map<String, Object> jobDataMap) {
//        //写锁加锁
//        lock.writeLock().lock();
//        try {
//            /**
//             *  JobDetail：任务的实现类以及类的信息
//             */
//            JobKey jobKey = new JobKey(jobName, jobGroupName);
//            JobDetail jobDetail;
//            //add a task (if this task already exists, return this task directly)
//            //添加任务（如果此任务已存在，则直接返回此任务）
//            if (scheduler.checkExists(jobKey)) {
//                jobDetail = scheduler.getJobDetail(jobKey);
//                if (jobDataMap != null) {
//                    jobDetail.getJobDataMap().putAll(jobDataMap);
//                }
//            } else {
//                jobDetail = JobBuilder.newJob(clazz).withIdentity(jobKey).build();
//
//                if (jobDataMap != null) {
//                    jobDetail.getJobDataMap().putAll(jobDataMap);
//                }
//
//                scheduler.addJob(jobDetail, false, true);
//
//                logger.info("Add job, job name: {}, group name: {}",
//                        jobName, jobGroupName);
//            }
//
//
//            /**
//             * 触发器 :   CronTrigger 实现更复杂的业务逻辑，比如每周五执行一次就用到了CronTrigger，实际上CronTrigger比SimpleTrigger更常用
//             *              simpleTrigger 定频率的执行某一个任务
//             */
//            TriggerKey triggerKey = new TriggerKey(jobName, jobGroupName);
//
//            /**
//             *
//             * Instructs the Scheduler that upon a mis-fire
//             * situation, the CronTrigger wants to have it's
//             * next-fire-time updated to the next time in the schedule after the
//             * current time (taking into account any associated Calendar),
//             * but it does not want to be fired now.
//             *
//             */
//
//            CronTrigger cronTrigger = TriggerBuilder.newTrigger().withIdentity(triggerKey).startAt(startDate).endAt(endDate)
//                    .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression).withMisfireHandlingInstructionDoNothing())
//                    .forJob(jobDetail).build();
//
//            //当触发器存在时，进行修改触发器，不存在时直接创建触发器
//            if (scheduler.checkExists(triggerKey)) {
//                // updateProcessInstance scheduler trigger when scheduler cycle changes
//                // 当计划程序周期更改时，updateProcessInstance计划程序触发器
//                CronTrigger oldCronTrigger = (CronTrigger) scheduler.getTrigger(triggerKey);
//                String oldCronExpression = oldCronTrigger.getCronExpression();
//                //当前cron表达式和老的表达式不相同时，重新安排触发器
//                if (!StringUtils.equalsIgnoreCase(cronExpression, oldCronExpression)) {
//                    /**
//                     *  scheduler 调度器
//                     *
//                     *  定时定频率的执行JobDetail的信息并且通过他将jobDetail和trigger绑定在一起，然后通过他来调用这些方法来执行业务
//                     *
//                     *  reschedule job trigger
//                     */
//                    scheduler.rescheduleJob(triggerKey, cronTrigger);
//                    logger.info("reschedule job trigger, triggerName: {}, triggerGroupName: {}, cronExpression: {}, startDate: {}, endDate: {}",
//                            jobName, jobGroupName, cronExpression, startDate, endDate);
//                }
//            } else {
//                /**
//                 *  scheduler 调度器
//                 *
//                 *  定时定频率的执行JobDetail的信息并且通过他将jobDetail和trigger绑定在一起，然后通过他来调用这些方法来执行业务
//                 */
//                scheduler.scheduleJob(cronTrigger);
//                logger.info("schedule job trigger, triggerName: {}, triggerGroupName: {}, cronExpression: {}, startDate: {}, endDate: {}",
//                        jobName, jobGroupName, cronExpression, startDate, endDate);
//            }
//
//
//        } catch (Exception e) {
//            logger.error("add job failed", e);
//            throw new RuntimeException("add job failed:" + e.getMessage());
//        } finally {
//            //最后必须释放锁
//            lock.writeLock().unlock();
//        }
//    }

    /**
     * add task trigger , if this task already exists, return this task with updated trigger
     *
     * @param clazz          job class name
     * @param jobName        job name
     * @param jobGroupName   job group name
     * @param cronExpression cron expression
     * @param jobDataMap     job parameters data map   可传可不传
     */
//    public void addJob(Class<? extends Job> clazz, String jobName, String jobGroupName,
//                       String cronExpression, Map<String, Object> jobDataMap) {
//        //写锁加锁
//        lock.writeLock().lock();
//        try {
//            /**
//             *  JobDetail：任务的实现类以及类的信息
//             */
//            JobKey jobKey = new JobKey(jobName, jobGroupName);
//            JobDetail jobDetail;
//            //add a task (if this task already exists, return this task directly)
//            //添加任务（如果此任务已存在，则直接返回此任务）
//            if (scheduler.checkExists(jobKey)) {
//                jobDetail = scheduler.getJobDetail(jobKey);
//
//                /**
//                 * 可传可不传
//                 */
//                if (jobDataMap != null) {
//                    jobDetail.getJobDataMap().putAll(jobDataMap);
//                }
//
//            } else {
//                jobDetail = JobBuilder.newJob(clazz).withIdentity(jobKey).build();
//
//                /**
//                 * 可传可不传
//                 */
//                if (jobDataMap != null) {
//                    jobDetail.getJobDataMap().putAll(jobDataMap);
//                }
//
//                scheduler.addJob(jobDetail, false, true);
//
//                logger.info("Add job, job name: {}, group name: {}",
//                        jobName, jobGroupName);
//            }
//
//
//            /**
//             * 触发器 :   CronTrigger 实现更复杂的业务逻辑，比如每周五执行一次就用到了CronTrigger，实际上CronTrigger比SimpleTrigger更常用
//             *              simpleTrigger 定频率的执行某一个任务
//             */
//            TriggerKey triggerKey = new TriggerKey(jobName, jobGroupName);
//
//            /**
//             *
//             * Instructs the Scheduler that upon a mis-fire
//             * situation, the CronTrigger wants to have it's
//             * next-fire-time updated to the next time in the schedule after the
//             * current time (taking into account any associated Calendar),
//             * but it does not want to be fired now.
//             *
//             */
//
//            CronTrigger cronTrigger = TriggerBuilder.newTrigger().withIdentity(triggerKey)
//                    .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression).withMisfireHandlingInstructionDoNothing())
//                    .forJob(jobDetail).build();
//
//            //当触发器存在时，进行修改触发器，不存在时直接创建触发器
//            if (scheduler.checkExists(triggerKey)) {
//                // updateProcessInstance scheduler trigger when scheduler cycle changes
//                // 当计划程序周期更改时，updateProcessInstance计划程序触发器
//                CronTrigger oldCronTrigger = (CronTrigger) scheduler.getTrigger(triggerKey);
//                String oldCronExpression = oldCronTrigger.getCronExpression();
//                //当前cron表达式和老的表达式不相同时，重新安排触发器
//                if (!StringUtils.equalsIgnoreCase(cronExpression, oldCronExpression)) {
//                    /**
//                     *  scheduler 调度器
//                     *
//                     *  定时定频率的执行JobDetail的信息并且通过他将jobDetail和trigger绑定在一起，然后通过他来调用这些方法来执行业务
//                     *
//                     *  reschedule job trigger
//                     */
//                    scheduler.rescheduleJob(triggerKey, cronTrigger);
//                    logger.info("reschedule job trigger, triggerName: {}, triggerGroupName: {}, cronExpression: {}",
//                            jobName, jobGroupName, cronExpression);
//                }
//            } else {
//                /**
//                 *  scheduler 调度器
//                 *
//                 *  定时定频率的执行JobDetail的信息并且通过他将jobDetail和trigger绑定在一起，然后通过他来调用这些方法来执行业务
//                 */
//                scheduler.scheduleJob(cronTrigger);
//                logger.info("schedule job trigger, triggerName: {}, triggerGroupName: {}, cronExpression: {}",
//                        jobName, jobGroupName, cronExpression);
//            }
//
//
//        } catch (Exception e) {
//            logger.error("add job failed", e);
//            throw new RuntimeException("add job failed:" + e.getMessage());
//        } finally {
//            //最后必须释放锁
//            lock.writeLock().unlock();
//        }
//    }


}
