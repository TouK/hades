package pl.touk.hades.camel;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;

public class HadesRouteBuilder extends RouteBuilder {

    @Override
    public void configure() throws Exception {
        System.out.println("foo before");
        from("quartz://hadesGroup/hadesTimerName/?fireNow=true&stateful=true")
                .log(LoggingLevel.INFO, HadesRouteBuilder.class.getName(), "camel quartz is working");
        System.out.println("foo after");
    }
}
