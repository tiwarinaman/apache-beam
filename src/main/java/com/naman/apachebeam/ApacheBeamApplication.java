package com.naman.apachebeam;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ApacheBeamApplication {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();

        System.out.println("Runner :" + options.getRunner().getName());
        System.out.println("JobName :" + options.getJobName());
        System.out.println("OptionsId :" + options.getOptionsId());
        System.out.println("StableUniqueName :" + options.getStableUniqueNames());
        System.out.println("TempLocation :" + options.getTempLocation());
        System.out.println("UserAgent :" + options.getUserAgent());


        SpringApplication.run(ApacheBeamApplication.class, args);
    }

}
