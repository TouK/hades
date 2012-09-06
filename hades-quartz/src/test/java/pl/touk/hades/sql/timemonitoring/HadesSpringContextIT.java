/*
* Copyright 2011 TouK
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package pl.touk.hades.sql.timemonitoring;

import org.junit.Test;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import pl.touk.hades.StringFactory;

import static org.junit.Assert.assertEquals;

/**
 * @author <a href="mailto:msk@touk.pl">Michal Sokolowski</a>
 */
public class HadesSpringContextIT {

    @Test
    public void shouldLoadSpringContextForHost1() throws SchedulerException, InterruptedException {

        ClassPathXmlApplicationContext ctx;

        ctx = new ClassPathXmlApplicationContext("/integration/context.xml");
        ctx.getBean("scheduler", Scheduler.class).start();

        String cronAlfa = "1 2 3 * * ?";
        String cronBeta = "2 3 4 * * ?";
        StringFactory.beansByName.put("cronAlfa", cronAlfa);
        StringFactory.beansByName.put("cronBeta", cronBeta);

        assertEquals(cronAlfa, ctx.getBean("monitorAlfa", SqlTimeBasedQuartzMonitor.class).getCron());
        assertEquals(cronBeta, ctx.getBean("monitorBeta", SqlTimeBasedQuartzMonitor.class).getCron());

        ctx.getBean("scheduler", Scheduler.class).shutdown();
        ctx.close();
    }
}