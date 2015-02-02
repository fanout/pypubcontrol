PyPubControl
============
Author: Justin Karneges <justin@fanout.io>

EPCP library for Python.

Requirements
------------

* pyjwt

Install
-------

You can install from PyPi:

    sudo pip install pubcontrol

Or from this repository:

    sudo python setup.py install

Sample usage
------------

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

# Wait for all async publish calls to complete:
pub.finish()
```
