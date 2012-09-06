package pl.touk.hades;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;

import java.util.HashMap;
import java.util.Map;

public class StringFactory implements BeanNameAware, FactoryBean {

    public final static Map<String, String> beansByName = new HashMap<String, String>();

    private String beanName;

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public Object getObject() {
        return beansByName.get(beanName);
    }

    public Class<?> getObjectType() {
        return String.class;
    }

    public boolean isSingleton() {
        return true;
    }
}
