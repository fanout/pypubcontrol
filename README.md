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
from pubcontrol import PubControl, Item, Format

pub = PubControl({
    'uri': 'https://api.fanout.io/realm/myrealm',
    'iss': realm_name,
    'key': b64decode(realm_key)
})

class HttpResponseFormat(Format):
    def __init__(self, body):
        self.body = body
    def name(self):
        return 'http-response'
    def export(self):
        return {'body': self.body}

pub.publish('mychannel', Item(HttpResponseFormat('stuff\n')))
```
