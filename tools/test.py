#!/usr/bin/python
# -*- coding: utf-8 -*-
#
#

from twisted.internet import reactor, protocol
from twisted.internet import defer
import logging
import sys

from twisted.protocols.memcache import MemCacheProtocol, DEFAULT_PORT
from random import random, shuffle

def df_sleep(tmout):
    df = defer.Deferred()
    reactor.callLater(tmout, df.callback, None)
    return df

def err_handler(x):
    print 'Err:', x
    reactor.stop()


@defer.inlineCallbacks
def itertest(proto):
    l1 = range(1000)
    l2 = range(1000)

    shuffle(l1)
    shuffle(l2)
    
    for i in l1: 
        res = yield proto.set("mykey_%d" % (i,), "a lot of data %d" % (i, ))
        #yield df_sleep(random())

    for i in l2:
        res = yield proto.get("mykey_%d" % (i,))
        assert res[1] == "a lot of data %d" % (i, )

    proto.transport.loseConnection()

@defer.inlineCallbacks
def do_test_async(nconns):
    print 'start'

    dl = []

    for i in range(nconns):
        df = protocol.ClientCreator(reactor, MemCacheProtocol ).connectTCP("localhost", 21201, timeout=300 ) 
        df.addCallback(itertest)
        dl.append(df)


    print '\nrunning tests (%d)...' % (len(dl),)

    res = yield defer.DeferredList(dl, consumeErrors=True)

    defer.returnValue('Pass: %d, Fail: %d' % (sum(1 for status, item in res if status),
                                              sum(1 for status, item in res if not status)))


def prn(x):
    print 'Done:', x
    reactor.stop()


def do_test():
    df = do_test_async(int(sys.argv[1]))
    df.addCallback(prn)
    df.addErrback(err_handler)


def main():
    reactor.callWhenRunning(do_test)
    reactor.run()


if __name__ == "__main__":
    main()
