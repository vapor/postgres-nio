# Listen & Notify

``PostgresNIO`` supports PostgreSQL's listen and notify API. Learn how to listen for changes and
notify other listeners.

## Overview

PostgreSQL provides simple publish and subscribe (Pub/Sub) messaging support using the `NOTIFY`, `LISTEN` and `UNLISTEN` commands.
It has the concept of a channel that a client can both publish to and subscribe to.
The server sends any notifications published to a channel to clients listening on that channel.
PostgreSQL channels are not persisted, for instance if a notification is sent to a channel that has no one listening, that notification is lost.

### Listening

Use ``PostgresConnection/listen(on:consume:)`` to listen for notifications on a given channel and receive every notification published to that channel via an `AsyncSequence`.
When you exit the closure provided, PostgresNIO sends the relevant `UNLISTEN` command.

```swift
try await connection.listen(on: "channel") { notifications in
    for try await notification in notifications {
        // a notification item includes the payload string communicated along with the notification
        print(notification.payload)
    }
}
```

### Notifying

You can send notifications to a channel using the `NOTIFY` command.

```swift
try await connection.query(#"NOTIFY "channel", 'bar';"#)
```

## Topics

- ``PostgresNotification``
- ``PostgresNotificationSequence``
