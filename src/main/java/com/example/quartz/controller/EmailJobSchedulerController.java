package com.example.quartz.controller;

import com.example.quartz.job.EmailJob;
import com.example.quartz.payload.ScheduleEmailRequest;
import com.example.quartz.payload.ScheduleEmailResponse;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.UUID;

/**
 * @author furg@senthink.com
 * @date 2018/11/26
 */
@RestController
public class EmailJobSchedulerController {

    private static final Logger logger = LoggerFactory.getLogger(EmailJobSchedulerController.class);

    @Autowired
    private Scheduler scheduler;

    @PostMapping("/scheduleEmail")
    public ResponseEntity<ScheduleEmailResponse> scheduleEmail(@Valid @RequestBody ScheduleEmailRequest request) {
        try {
            ZonedDateTime dateTime = ZonedDateTime.of(request.getDateTime(), request.getTimeZone());
            if (dateTime.isBefore(ZonedDateTime.now())) {
                ScheduleEmailResponse response = new ScheduleEmailResponse(false, "dataTime must be after current time");
                return ResponseEntity.badRequest().body(response);
            }

            JobDetail jobDetail = buildJobDetail(request);
            Trigger trigger = buildJobTrigger(jobDetail, dateTime);
            scheduler.scheduleJob(jobDetail, trigger);

            ScheduleEmailResponse response = new ScheduleEmailResponse(true, jobDetail.getKey().getName(),
                    jobDetail.getKey().getGroup(), "Email Scheduled Successfully");
            return ResponseEntity.ok(response);

        } catch (SchedulerException e) {
            logger.error("ERROR SCHEDULER EMAIL", e);
            ScheduleEmailResponse response = new ScheduleEmailResponse(false, "Error scheduling email, please try later!");
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }

    private Trigger buildJobTrigger(JobDetail jobDetail, ZonedDateTime dateTime) {
        return TriggerBuilder.newTrigger()
                .forJob(jobDetail)
                .withIdentity(jobDetail.getKey().getName(), "email-triggers")
                .withDescription("Send Email Trigger")
                .startAt(Date.from(dateTime.toInstant()))
                .withSchedule(SimpleScheduleBuilder.simpleSchedule().withMisfireHandlingInstructionFireNow())
                .build();
    }

    private JobDetail buildJobDetail(ScheduleEmailRequest request) {

        JobDataMap map = new JobDataMap();

        map.put("email", request.getEmail());
        map.put("subject", request.getSubject());
        map.put("body", request.getBody());


        return JobBuilder.newJob(EmailJob.class)
                .withIdentity(UUID.randomUUID().toString(), "email-josbs")
                .withDescription("Send Email Job")
                .usingJobData(map)
                .storeDurably()
                .build();
    }
}
