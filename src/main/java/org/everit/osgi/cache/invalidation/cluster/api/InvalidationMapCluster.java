package org.everit.osgi.cache.invalidation.cluster.api;

/**
 * Cluster handler for the {@link org.everit.osgi.cache.invalidation.InvalidationMap}.
 */
public interface InvalidationMapCluster extends InvalidationMapCallback {

  /**
   * Returns the ping message period.
   *
   * @return The ping message period in milliseconds.
   * @see #setPingPeriod(long)
   */
  long getPingPeriod();

  /**
   * Returns the sync check delay.
   *
   * @return The sync check delay.
   * @see #setSynchCheckDelay(long)
   */
  long getSynchCheckDelay();

  /**
   * Sets the ping message scheduler period.
   *
   * @param period
   *          The period in milliseconds.
   */
  void setPingPeriod(long period);

  /**
   * Sets the synch check delay in milliseconds. If the handler detects a potential message loss (or
   * swap) schedules a message sync check in the time set.
   *
   * @param synchCheckDelay
   *          The synch check delay.
   */
  void setSynchCheckDelay(long synchCheckDelay);

  /**
   * Starts the clustered operation.
   *
   * @param stateTimeout
   *          The timeout in milliseconds of the starting method.
   */
  void start(long stateTimeout);

  /**
   * Stops the clustered operation.
   */
  void stop();

}
