/*
 * Copyright (C) 2011 Everit Kft. (http://www.everit.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.everit.osgi.cache.jchannel.internal;

import java.util.Arrays;

import org.jgroups.Channel;
import org.jgroups.ChannelListener;
import org.jgroups.MembershipListener;
import org.jgroups.MessageListener;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.MethodLookup;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;

/**
 * Abstract channel handler for the {@link AbstractInvalidateMap}.
 *
 * @param <S>
 *          The type of the remote method handler object.
 */
public abstract class AbstractChannelHandler<S>
    implements MessageListener, MembershipListener, ChannelListener {

  /**
   * The backing channel.
   */
  private Channel channel;

  /**
   * The remote method call dispather on the top of the {@link #channel}.
   */
  private RpcDispatcher dispatcher = null;

  /**
   * Remote call options.
   */
  private final RequestOptions callOptions = new RequestOptions(ResponseMode.GET_ALL, 500);

  /**
   * Server object.
   */
  private final S server;

  /**
   * Method lookup in the server.
   */
  private final MethodLookup methods;

  /**
   * Creates the instance.
   *
   * @param channel
   *          The channel instance.
   * @param server
   *          The server.
   * @param methods
   *          The method lookup that maps the method ID to the method in the server.
   */
  protected AbstractChannelHandler(final Channel channel, final S server,
      final MethodLookup methods) {
    this.channel = channel;
    this.server = server;
    this.methods = methods;
  }

  /**
   * Calls a method remotely over the channel.
   *
   * @param id
   *          The ID of the method.
   * @param args
   *          The arguments.
   */
  protected void callRemoteMethod(final short id, final Object... args) {
    MethodCall call = new MethodCall(id, args);
    try {
      dispatcher.callRemoteMethods(null, call, callOptions);
    } catch (Exception e) {
      throw new RuntimeException(
          "Cannot call " + methods.findMethod(id) + " with parameters " + Arrays.toString(args),
          e);
    }
  }

  /**
   * Returns the cluster timeout.
   *
   * @return The cluster timeout
   * @see AbstractChannelHandler#setTimeout(long)
   */
  public long getTimeout() {
    return callOptions.getTimeout();
  }

  /**
   * Returns the blocking update mode flag.
   *
   * @return The flag.
   * @see AbstractChannelHandler#setBlockingUpdates(boolean)
   */
  public boolean isBlockingUpdates() {
    return callOptions.getMode() == ResponseMode.GET_ALL;
  }

  /**
   * Sets the blocking update mode. If blocking update is active, the cluster call will wait for
   * response from every node.
   *
   * @param blockingUpdates
   *          Blocking update enable flag.
   */
  public void setBlockingUpdates(final boolean blockingUpdates) {
    callOptions.setMode(blockingUpdates ? ResponseMode.GET_ALL : ResponseMode.GET_NONE);
  }

  /**
   * Sets the cluster call timeout (until all acknowledgement have been received).
   *
   * @param timeout
   *          The timeout in milliseconds for blocking updates
   */
  public void setTimeout(final long timeout) {
    callOptions.setTimeout(timeout);
  }

  /**
   * Starts the channel handler.
   *
   * @param stateTimeout
   *          The timeout in milliseconds of the starting method.
   * @throws Exception
   *           If any error occurred.
   */
  public final void start(final long stateTimeout) throws Exception {
    if (dispatcher != null) {
      return;
    }
    channel.setDiscardOwnMessages(true);
    channel.addChannelListener(this);
    dispatcher = new RpcDispatcher(channel, this, this, server);
    dispatcher.setMethodLookup(methods);
    channel.getState(null, stateTimeout);
  }

  /**
   * Stops the channel handler.
   */
  public void stop() {
    if (dispatcher != null) {
      dispatcher.stop();
      dispatcher = null;
    }
    channel.removeChannelListener(this);
  }

}
