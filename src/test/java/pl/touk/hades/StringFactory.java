package pl.touk.hades;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.FactoryBean;

import java.util.HashMap;
import java.util.Map;

public class StringFactory implements BeanNameAware, FactoryBean {

    public final static Map<String, Object> beansByName = new HashMap<String, Object>();

    private String beanName;

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }

    public Object getObject() {
        return beansByName.get(beanName);
    }

    public Class<?> getObjectType() {
        return beansByName.get(beanName).getClass();
    }

    public boolean isSingleton() {
        return true;
    }
}
