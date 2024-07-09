#  Copyright (c) 2020 SBA - MIT License

import selectors
from collections.abc import Callable
from selectors import SelectorKey, EVENT_READ
from typing import Optional, List, Tuple, Iterable, Union
from unittest.mock import Mock
import socket
import collections.abc


def _gen_uniq(start: int):
    """ Helper to generate unique values.

    Example usage:
    gen_fd = _gen_uniq(10)
    a = gen_fd()            # will be 10
    b = gen_fd()            # will be 11

    :param start: first value that will be returned
    :type start: int
    :return: a function that will return a different integer at each call
    (starting from the start parameter)
    :rtype: function
    """
    i = start

    def inner():
        nonlocal i
        cr = i
        i = i + 1
        return cr

    return inner


_gen_fd = _gen_uniq(10)


class MockSocket(Mock):
    """ Subclass of unittest.mock.Mock aimed at accepted TCP sockets.

    A MockSocket receives at creation time a list of byte strings. Those
    byte strings will be returned, one at a time when the recv method will
    be called. If one string is longer than the bufsize parameter, only a
    fragment of maximum allowed size will be returned. The remaining part
    of the byte string is then available for the future recv calls.

    Different MockSocket objects will all have different fileno numbers.
    """

    def __new__(cls, *_args, **kwargs):
        obj = super().__new__(cls, socket.socket)
        return obj

    def __init__(self, recvs: Iterable[Union[bytes, Callable]] = None):
        """
        :param recvs: iterable of byte strings or callable returning byte
         strings to be returned by recv calls
        :type recvs: Iterable[bytes]
        """
        super().__init__(socket.socket)
        if recvs is None:
            recvs = []
        self.recvs = iter(recvs)
        self._fileno = _gen_fd()
        self.remain = ''

    def recv(self, size):
        if len(self.remain) > 0:
            data = self.remain
        else:
            data = next(self.recvs, b'')
            if isinstance(data, Callable):
                data = data()
        if size < len(data):
            data, self.remain = data[:size], data[size:]
        else:
            self.remain = ''
        return data

    def fileno(self):
        return self._fileno

    def _get_child_mock(self, **kw):
        return Mock(**kw)


class ListenSocket:
    """ A class aimed at mocking listening TCP sockets.

    It receives at creation time an iterable of socket objects or callable
    returning socket objects. They will be returned in order by accept calls.

    As a special case, Mock objects are not processed as callable to allow
    smooth handling of MockSocket or Mock(socket.socket) objects.

    A proper sequence bind -> listen -> accept, ... -> close is required.
    An OSError is raised if accept is used before bind and listen or if
    any call is used on a closed socket
    """

    def __init__(self, accepted: Iterable[Union[socket.socket,
                                                Callable]] = None):
        """
        :param accepted: Iterable of socket objects or callable returning
        socket objects to return in sequence when accept is called
        :type accepted: Iterable[socket.socket]
        """
        accepted = [] if accepted is None else accepted
        self.accepted = iter(accepted)
        self.current = 0
        self.state = 0
        self._fileno = _gen_fd()
        self.address = 'localhost'
        self.remote_port = 57000

    def _addr(self):
        self.remote_port += 1
        return self.address, self.remote_port

    def bind(self, _address):
        if not (self.state <= 1):
            raise OSError()
        self.state = 1

    def listen(self, _backlog=5):
        if not (self.state == 1):
            raise OSError
        self.state = 2

    def close(self):
        self.state = 3

    def accept(self):
        if not (self.state == 2):
            raise OSError
        c = next(self.accepted, MockSocket())
        if isinstance(c, Callable) and not isinstance(c, Mock):
            c = c()
        self.current += 1
        return c, self._addr()

    def fileno(self):
        return self._fileno


# noinspection PyProtectedMember
class MockSelector(selectors._BaseSelectorImpl):
    """ BaseSelector subclass to help building tests on TCP servers.

    A MockSelector object is created with an iterable of events that will
    be returned by its select method. Here an event can be:
    - a ListenSocket or socket object. The object should have been
    registered at the time when select will return the event. The
    associated(key, event) tuple will use EVENT_READ
    - a 2-tuple containing an object as described above and the selector
    event (combination of EVENT_READ, EVENT_WRITE) to use
    - an iterable containing above objects or pair. In that case, select
    will return a corresponding list of (key, event) pairs. As a special
    case, an empty iterable will simulate a timeout on the selector by
     returning an empty list.
    """

    class EndException(BaseException):
        """ Internal exception raised when the event iterable is exhausted.

        It is intended to allow a MockSelector to be used as a context manager
        around an otherwise never ending loop: when the event iterable is
        exhausted, the context block filters its own EndException and the
        execution proceeds normally.
        """
        pass

    def __init__(self, event_list: Iterable = None):
        event_list = [] if event_list is None else event_list
        self.iter_event = iter(event_list)
        super().__init__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type == MockSelector.EndException and exc_val.args[0] is self:
            return True
        return False

    def select(self, _timeout: Optional[float] = ...) -> List[Tuple[SelectorKey, int]]:
        try:
            ev = next(self.iter_event)
        except StopIteration as e:
            raise MockSelector.EndException(self) from e
        if not isinstance(ev, collections.abc.Iterable):
            ev = (ev,)
        try:
            if isinstance(ev[1], int):
                ev = (ev,)
        except (KeyError, IndexError):
            pass
        kevs = []
        for e in ev:
            if isinstance(e, tuple):
                sock, event = e
            else:
                sock, event = e, EVENT_READ
            k = self.get_map()[sock]
            kevs.append((k, event))
        return kevs
