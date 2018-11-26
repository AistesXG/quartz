package com.example.quartz.payload;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

/**
 * @author furg@senthink.com
 * @date 2018/11/26
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class ScheduleEmailResponse {

    private boolean success;

    private String jobId;

    private String jobGroup;

    private String message;

    public ScheduleEmailResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    public ScheduleEmailResponse(boolean success, String jobId, String jobGroup, String message) {
        this.success = success;
        this.jobId = jobId;
        this.jobGroup = jobGroup;
        this.message = message;
    }
}
