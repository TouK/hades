/**
 * Provides HA data source that switches between the two contained data sources (the main one and the failover one) on
 * the basis of their load levels comparison. The load level of a data source is determined by the average execution
 * time of some configurable sql statement - the smaller the average, the lower the load level. The comparison of load
 * levels can employ hysteresis (two load levels can produce different states depending on the previous states), but
 * roughly speaking the failover data source is used when the main one is overloaded. If hysteresis is employed the
 * failover data source is used also when the main one is recovering after overload.
 */
package pl.touk.hades.load;
