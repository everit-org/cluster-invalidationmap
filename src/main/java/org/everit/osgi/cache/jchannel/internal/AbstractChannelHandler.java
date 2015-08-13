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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
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
public abstract class AbstractChannelHandler<S> extends ReceiverAdapter {

  /**
   * Members' states.
   */
  private static enum State {

    /**
     * Member is accepted.
     */
    ACCEPTED,

    /**
     * Regular disconnect message detected.
     */
    DISCONNECTED,

    /**
     * Crashed (received a suspected event, or disconnected without regular disconnect message.
     */
    CRASHED
  }

  private static final Logger LOGGER = Logger.getLogger(AbstractChannelHandler.class.getName());

  /**
   * The backing channel.
   */
  private Channel channel;

  /**
   * The remote method call dispatcher on the top of the {@link #channel}.
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
   * Member registry.
   */
  private final ConcurrentMap<Address, State> members = new ConcurrentHashMap<>();

  /**
   * Cluster dropped out flag.
   */
  private boolean droppedOut = false;

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
   * Calls a method remotely over the channel. Does nothing if {@link #isDroppedOut()} flag is set.
   *
   * @param id
   *          The ID of the method.
   * @param args
   *          The arguments.
   */
  protected void callRemoteMethod(final short id, final Object... args) {
    if (droppedOut) {
      return;
    }
    try {
      MethodCall call = new MethodCall(id, args);
      dispatcher.callRemoteMethods(null, call, callOptions);
      LOGGER.info("Method called: " + call);
    } catch (Exception e) {
      throw new RuntimeException(
          "Cannot call " + methods.findMethod(id) + " with parameters " + Arrays.toString(args),
          e);
    }
  }

  /**
   * Checks the connection state. Also invokes {@link #droppedOut()} or {@link #reConnected()} if
   * necessary.
   */
  private void checkConnection() {
    Iterator<State> stateIt = new HashSet<>(members.values()).iterator();
    if (stateIt.hasNext() && State.CRASHED.equals(stateIt.next()) && !stateIt.hasNext()) {
      // have members but only in crashed state
      if (!droppedOut) {
        // mark drop out if drop out detected firstly in this situation
        droppedOut = true;
        droppedOut();
        LOGGER.info("Dropped out from the cluster.");
      }
    } else if (droppedOut) {
      // mark reconnection if has no members or have not only crashed members and drop out has been
      // detected earlier.
      droppedOut = false;
      reConnected();
      LOGGER.info("Reconnected to the cluster.");
    }
  }

  /**
   * Listener for detect cluster drop out.
   */
  public abstract void droppedOut();

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

  public boolean isDroppedOut() {
    return droppedOut;
  }

  /**
   * Maintains the members registry.
   *
   * @param view
   *          The view from the actually connected members can be got.
   */
  private void maintainMembers(final View view) {
    LOGGER.info("Maintaining members...");

    // handle the already registered members
    Iterator<Entry<Address, State>> membersIt = members.entrySet().iterator();
    while (membersIt.hasNext()) {
      Entry<Address, State> member = membersIt.next();
      Address memberAddress = member.getKey();
      State memberState = member.getValue();
      boolean memberPresentInView = view.containsMember(memberAddress);

      switch (memberState) { // examine registered members by the last state
        case ACCEPTED:
          // switch to crashed if the last state is accepted but does not present in the view (did
          // not give the regular disconnect message before disconnected)
          if (!memberPresentInView) {
            member.setValue(State.CRASHED);
            LOGGER.info("Member leaved without sending disconnect message " + memberAddress);
          }
          break;
        case CRASHED:
          // switch to accepted if previous state is crashed, but present in the new view
          if (memberPresentInView) {
            member.setValue(State.ACCEPTED);
            LOGGER.info("Member accepted " + memberAddress);
          }
          break;
        case DISCONNECTED:
          // remove member if previous state is regularly disconnected, and does not present in the
          // new view
          if (!memberPresentInView) {
            LOGGER.info("Member disconnected " + memberAddress);
            membersIt.remove();
          }
          break;
        default:
          // must not happen
          throw new RuntimeException("illegal member state: " + memberState.name());
      }
    }

    Address self = channel.getAddress();
    // set accepted state on members were not registered but are in the view
    view.getMembers().forEach(memberAddress -> {
      if (self.equals(memberAddress)) {
        return;
      }
      State oldState = members.putIfAbsent(memberAddress, State.ACCEPTED);
      if (oldState == null) {
        LOGGER.info("Member accepted " + memberAddress);
      }
    });

  }

  /**
   * {@inheritDoc} Also handles the regular disconnect message. This method shall be invoked from an
   * override method.
   */
  @Override
  public void receive(final Message msg) {
    LOGGER.info("Message was got " + msg);
    Object object = msg.getObject();
    if (State.DISCONNECTED.name().equals(object)) {
      members.put(msg.getSrc(), State.DISCONNECTED);
      LOGGER.info("Member disconnect message was recognized, disconnect... " + msg.getSrc());
    }
  }

  /**
   * Listener for detect cluster reconnection.
   */
  public abstract void reConnected();

  /**
   * Sends the regular disconnect message.
   */
  public void sendDisconnect() {
    Address self = channel.getAddress();
    channel.getView().getMembers().forEach(memberAddress -> {
      if (self.equals(memberAddress)) {
        return;
      }
      try {
        channel.send(memberAddress, State.DISCONNECTED.name());
        LOGGER.info("Member disconnect message was sent " + memberAddress);
      } catch (Exception e) {
        LOGGER.log(Level.WARNING, "Member disconnect message cannot be sent " + memberAddress, e);
      }
    });
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
   * <p>
   * Note: Channel must discard echo messages so this method invokes
   * {@link Channel#setDiscardOwnMessages(boolean)} with the parameter <code>true</code>.
   * <strong> Do not modify this flag while the map is in the cluster!</strong>
   * </p>
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
    dispatcher = new RpcDispatcher(channel, this, this, server);
    dispatcher.setMethodLookup(methods);
    channel.getState(null, stateTimeout);
    maintainMembers(channel.getView());
    checkConnection();
    LOGGER.info("Channel was started");
  }

  /**
   * Stops the channel handler.
   */
  public final void stop() {
    if (dispatcher != null) {
      sendDisconnect();
      dispatcher.stop();
      dispatcher = null;
      LOGGER.info("Channel was stopped");
    }
  }

  @Override
  public void suspect(final Address suspectedMember) {
    members.put(suspectedMember, State.CRASHED);
    LOGGER.info("Member crashed " + suspectedMember);
  }

  @Override
  public void viewAccepted(final View newView) {
    maintainMembers(newView);
    checkConnection();
  }

}
