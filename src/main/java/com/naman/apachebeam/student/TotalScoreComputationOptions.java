package com.naman.apachebeam.student;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface TotalScoreComputationOptions extends PipelineOptions {

    @Description("Path of the file to read from")
    @Default.String("src/main/resources/student_scores.csv")
    String getInputFile();

    void setInputFile(String value);

    @Description("Path of the file to write to")
    @Validation.Required
    String getOutputFile();

    void setOutputFile(String value);

    @Validation.Required
    String getProjectId();

    void setProjectId(String value);

    @Validation.Required
    String getTopic();

    void setTopic(String value);

}
