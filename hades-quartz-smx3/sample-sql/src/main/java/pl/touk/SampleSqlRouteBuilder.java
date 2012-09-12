package pl.touk;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;

public class SampleSqlRouteBuilder extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        from("ref:sampleSql")
                .setBody(constant("SELECT SYSDATE FROM DUAL"))
                .to("jdbc:hades_PC1_PC2")
                .split(body())
                .log(LoggingLevel.INFO, SampleSqlRouteBuilder.class.getName(), "date: ${body[SYSDATE]}");
    }
}
