# PyPubControl

Authors: Justin Karneges <justin@fanout.io>, Konstantin Bokarius <kon@fanout.io>

EPCP library for Python.

## Install

You can install from PyPi:

```sh
pip install pubcontrol
```

Or from this repository:

```sh
python setup.py install
```

NOTE: ZMQ publishing requires the `pyzmq` and `tnetstring` (`tnetstring3` for Python 3) packages to be installed.

## Sample usage

```python
from base64 import b64decode
from pubcontrol import PubControl, PubControlClient, Item, Format

class HttpResponseFormat(Format):
    def __init__(self, body):
        self.body = body
    def name(self):
        return 'http-response'
    def export(self):
        return {'body': self.body}

def callback(result, message):
    if result:
        print('Publish successful')
    else:
        print('Publish failed with message: ' + message)

# PubControl can be initialized with or without an endpoint configuration.
# Each endpoint can include optional JWT authentication info.
# Multiple endpoints can be included in a single configuration.

# Initialize PubControl with a single endpoint:
pub = PubControl({'uri': 'https://api.fanout.io/realm/<myrealm>',
        'iss': '<myrealm>', 'key': b64decode('<realmkey>')})

# Add new endpoints by applying an endpoint configuration:
pub.apply_config([{'uri': '<myendpoint_uri_1>'},
        {'uri': '<myendpoint_uri_2>'}])

# Remove all configured endpoints:
pub.remove_all_clients()

# Explicitly add an endpoint as a PubControlClient instance:
pubclient = PubControlClient('<myendpoint_uri>')
# Optionally set JWT auth: pubclient.set_auth_jwt(<claim>, '<key>')
# Optionally set basic auth: pubclient.set_auth_basic('<user>', '<password>')
pub.add_client(pubclient)

# Publish across all configured endpoints:
pub.publish('<channel>', Item(HttpResponseFormat('Test publish!')))
pub.publish('<channel>', Item(HttpResponseFormat('Test async publish!')),
        blocking=False, callback=callback)
```

## Requiring Subscribers

You can configure `PubControl` to require subscribers when publishing messages in both `PubControlClient` and `PubControl`. When requiring subscribers, the internal `PubSubMonitor` class is used to keep track of all subscribed-to channels and acts as a filter to prevent messages from being published to channels that have no subscribers. Note that a message published to non-subscribed-to channel does not result in a failure - the message is simply dropped and a successful result is sent back to the caller.

Using `PubControlClient`:

```python
def callback(type, channel):
    if type == "sub":
        print(channel + " has subscribers")
    else:
        print(channel + " no longer has subscribers")

pub = PubControlClient('https://api.fanout.io/realm/<myrealm>',
        {'iss': '<myrealm>'}, b64decode('<realmkey>'), True, callback)
```

Using `PubControl`:

```python
pub = PubControl({
    'uri': 'https://api.fanout.io/realm/<myrealm>',
    'iss': '<myrealm>', 'key': b64decode('<realmkey>'),
    'require_subscribers': True
})
```

## ZMQ Publishing

This library supports publishing to ZMQ sockets via the `PubControl` class or via the `ZmqPubControlClient` class directly. Both XPUB and PUSH sockets are supported. XPUB sockets are only published to when the publishing channel has subscribers while PUSH sockets are always published to. To indicate that an XPUB socket should be used set `require_subscribers` to `True`.

While you can explicitly specify the PUSH and XPUB socket URIs, the recommended approach is to use the command socket URI for automatically discovering both the PUSH and XPUB socket URIs. By default, Pushpin's command socket listens on port 5563. To use the command socket, specify the URI in the `PubControl` config as shown in the snippet below, or if using `ZmqPubControlClient` directly then set the `uri` constructor parameter. Automatic discovery of the PUSH and XPUB socket URIs will then occur asynchronously and should complete within a couple seconds.

NOTE: ZMQ publishing requires the `pyzmq` and `tnetstring` (`tnetstring3` for Python 3) packages to be installed.

```python
# Initialize PubControl with a ZMQ command URI and indicate that the XPUB socket
# should be used via the require_subscribers key:
pub = PubControl({
    'zmq_uri': 'tcp://localhost:5563',
    'require_subscribers': True
})
```
