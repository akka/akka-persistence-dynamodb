/*
 * Copyright (C) 2024 Lightbend Inc. <https://www.lightbend.com>
 */

package docs.javadsl;

import akka.persistence.typed.PersistenceId;
import akka.persistence.typed.javadsl.CommandHandler;
import akka.persistence.typed.javadsl.EventHandler;
import akka.persistence.typed.javadsl.EventSourcedBehavior;

public class MultiPluginDocExample {

  public interface Command {}

  public interface Event {}

  public static class State {}

  // #with-plugins
  public class MyEntity extends EventSourcedBehavior<Command, Event, State> {
    // #with-plugins
    public MyEntity(PersistenceId persistenceId) {
      super(persistenceId);
    }

    @Override
    public State emptyState() {
      return new State();
    }

    @Override
    public CommandHandler<Command, Event, State> commandHandler() {
      return newCommandHandlerBuilder().build();
    }

    @Override
    public EventHandler<State, Event> eventHandler() {
      return newEventHandlerBuilder().build();
    }

    // #with-plugins
    @Override
    public String journalPluginId() {
      return "second-dynamodb.journal";
    }

    @Override
    public String snapshotPluginId() {
      return "second-dynamodb.snapshot";
    }
  }
  // #with-plugins

}
