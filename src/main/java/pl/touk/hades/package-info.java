/**
 * Provides a HA (high-availability) data source. This HA data source contains two other data sources - the main one
 * and the failover one. The high-availability effect is achieved by switching between the two data sources. For
 * example if the main data source breaks, the failover one replaces it until the situation is back to normal.
 * <p>
 * The process of switching between the two contained data sources is not controlled by the HA data source itself.
 * Implementations of {@link pl.touk.hades.FailoverActivator} are responsible for this. Every HA data source is
 * associated with such an implementation which activates failover (switch from the main data source to the failover
 * one) or failback (the opposite switch) when some conditions are met. The conditions may vary between implementations.
 * For example {@link pl.touk.hades.load.LoadFailoverActivator} activates failover when the main data source is
 * overloaded.
 */
package pl.touk.hades;