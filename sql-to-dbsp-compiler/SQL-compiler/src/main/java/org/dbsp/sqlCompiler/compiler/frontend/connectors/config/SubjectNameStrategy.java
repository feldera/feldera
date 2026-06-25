package org.dbsp.sqlCompiler.compiler.frontend.connectors.config;
import com.fasterxml.jackson.annotation.JsonProperty;

public enum SubjectNameStrategy {
    @JsonProperty("topic_name")        TopicName,
    @JsonProperty("record_name")       RecordName,
    @JsonProperty("topic_record_name") TopicRecordName,
}
