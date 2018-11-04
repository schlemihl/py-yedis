import datetime
from itertools import chain
from math import isinf
import pytz
from redis._compat import imap, izip
from redis.client import StrictRedis, bool_ok, dict_merge, string_keys_to_dict
from redis.connection import Token
from redis.exceptions import RedisError

m_inf = float('-inf')
p_inf = float('inf')


class DatetimeToTimestamp(object):
    """
    Return a datetime ``tm`` as a timestamp in milliseconds since the epoch.

    If the datetime is naive it is assumed to be UTC unless ``tz`` is set
    different. ``None`` will result in UTC too.
    """

    _epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)

    def __init__(self, tz=pytz.utc):
        if tz is None:
            self._tz = pytz.utc
        else:
            self._tz = tz

    def __call__(self, tm):
        if tm.tzinfo is None:
            t = self._tz.localize(tm)
            return int((t - self._epoch).total_seconds() * 1e3)
        return int((tm - self._epoch).total_seconds() * 1e3)


def unixtime_to_timestamp(tm):
    """
    Return the time in seconds since the epoch (unix time)
    as a floating point number ``tm``
    as a timestamp in milliseconds since the epoch.

    Note: The unix time is naive, that is, it doesn't know its timezone.
    ``tm`` MUST be in UTC!
    """
    return int(tm * 1000)


class TimestampToDatetime(object):
    """
    Return a timestamp in milliseconds since the epoch as a datetime.

    The datetime is in UTC unless ``tz`` is set different. ``None`` will
    result in UTC too.
    """

    def __init__(self, tz=pytz.utc):
        if tz is None:
            self._tz = pytz.utc
        else:
            self._tz = tz

    def __call__(self, ts):
        return datetime.datetime.fromtimestamp(timestamp_to_unix(ts), self._tz)


def timestamp_to_unix(ts):
    """
    Return the timestamp in milliseconds since the epoch ``ts``
    as a time in seconds since the epoch (unix time)
    as a floating point number.
    """
    return int(ts) * 1e-3


def timeseries_time_value_pairs(response, **options):
    """
    Return the ``response`` as a list of (time, value) pairs.

    ``options`` may contain a callable ``timestamp_cast_func``
    used to cast the timestamp return values to times.
    Timestamps are milliseconds since the epoch as 64 bit signed integers.
    """
    if not response:
        return response
    timestamp_cast_func = options.get('timestamp_cast_func', int)
    it = iter(response)
    return list(izip(imap(timestamp_cast_func, it), it))


class Yedis(StrictRedis):
    """Provides access to timeseries via the Yedis API of YugabyteDB."""

    # Overridden callbacks
    RESPONSE_CALLBACKS = dict_merge(
        StrictRedis.RESPONSE_CALLBACKS,
        string_keys_to_dict(
            'TSADD TSREM',
            bool_ok
        ),
        string_keys_to_dict(
            'TSCARD',
            int
        ),
        string_keys_to_dict(
            'TSLASTN TSRANGEBYTIME TSREVRANGEBYTIME',
            timeseries_time_value_pairs
        )
    )

    def tsadd(self, name, *times_and_values, **options):
        """
        Add any number of ``time``, ``value`` pairs to the time series
        that is specified by the given key ``name``.

        Pairs can be specified as ``times_and_values``, in the form of:
        ``time1``, ``value1``, ``time2``, ``value2``,...

        This is useful in storing time series like data where the ``name``
        could define a metric, the ``time`` is the time when the metric was
        generated and ``value`` is the value of the metric at the given time.

        ``times`` can be of different types. They are converted to
        timestamps - 64 bit signed integers - using a callable
        ``time_cast_func`` within options.

        ``times`` should be python datetimes if ``time_cast_func``
        isn't set. A naive datetime is accounted an UTC datetime.

        ``times`` could be the time in seconds since the epoch (unix time)
        as a floating point number in case ``time_cast_func`` is
        set to ``unixtime_to_timestamp`` for instance.
        Note: The unix time is naive, that is, it doesn't know its timezone.
        ``times`` MUST be in UTC!

        To provide raw timestamps set ``time_cast_func`` to None.
        """
        pieces = ['TSADD', name]
        if times_and_values:
            if len(times_and_values) % 2 != 0:
                raise RedisError("TSADD requires an equal number of "
                                 "times and values")
            time_cast_func = options.get('time_cast_func',
                                         DatetimeToTimestamp(pytz.utc))
            if time_cast_func is None:
                pieces.extend(times_and_values)
            else:
                pieces.extend(
                    chain.from_iterable(izip(imap(time_cast_func,
                                                  times_and_values[0::2]),
                                             times_and_values[1::2])))
        return self.execute_command(*pieces)

    def tsrem(self, name, *times, **options):
        """
        Removes one or more specified ``times`` from the time series
        that is specified by the given key ``name``.

        ``times`` can be of different types. They are converted to
        timestamps - 64 bit signed integers - using a callable
        ``time_cast_func`` within options.

        ``times`` should be python datetimes if ``time_cast_func``
        isn't set. A naive datetime is accounted an UTC datetime.

        ``times`` could be the time in seconds since the epoch (unix time)
        as a floating point number in case ``time_cast_func`` is
        set to ``unixtime_to_timestamp`` for instance.
        Note: The unix time is naive, that is, it doesn't know its timezone.
        ``times`` MUST be in UTC!

        To provide raw timestamps set ``time_cast_func`` to None.
        """
        pieces = ['TSREM', name]
        if times:
            time_cast_func = options.get('time_cast_func',
                                         DatetimeToTimestamp(pytz.utc))
            if time_cast_func is None:
                pieces.extend(times)
            else:
                pieces.extend(imap(time_cast_func, times))
        return self.execute_command(*pieces)

    def tscard(self, name):
        """
        Return the number of entires in the time series
        that is specified by the given key ``name``.
        """
        return self.execute_command('TSCARD', name)

    def tsget(self, name, tm, time_cast_func=DatetimeToTimestamp(pytz.utc)):
        """
        Return the value for the given time ``tm`` in the time series
        that is specified by the given key ``name``.

        ``time_cast_func`` a callable used to cast the time ``tm``
        to a timestamp - 64 bit signed integer (cf. ``tsadd``).
        """
        if time_cast_func is None:
            return self.execute_command('TSGET', name, tm)
        return self.execute_command('TSGET', name, time_cast_func(tm))

    def tslastn(self, name, num,
                timestamp_cast_func=TimestampToDatetime(pytz.utc)):
        """
        Returns the latest ``num`` entries in the time series
        that is specified by the given key ``name``.

        The entries are returned in ascending order of the times.

        ``timestamp_cast_func`` a callable used to cast the timestamp
        return values. It should reflect how timestamp were inserted
        (cf. ``time_cast_func``).
        """
        if timestamp_cast_func is None:
            return self.execute_command('TSLASTN', name, num,
                                        timestamp_cast_func=int)
        return self.execute_command('TSLASTN', name, num,
                                    timestamp_cast_func=timestamp_cast_func)

    def tsrangebytime(self, name, tm_low=None, tm_high=None,
                      time_cast_func=DatetimeToTimestamp(pytz.utc),
                      timestamp_cast_func=TimestampToDatetime(pytz.utc)):
        """
        Returns the entries for the given time range
        from ``tm_low`` to ``tm_high`` in the time series
        that is specified by the given key ``name``.

        The entries are returned in ascending order of the times.

        Special bounds -inf (``tm_low`` is None or -inf) and
        +inf (``tm_high`` is None or inf) are also supported to retrieve
        an entire range.

        ``time_cast_func`` a callable used to cast the time ``tm``
        to a timestamp - 64 bit signed integer (cf. ``tsadd``).

        ``timestamp_cast_func`` a callable used to cast the timestamp
        return values. It should reflect how timestamp were inserted
        (cf. ``time_cast_func``).
        """
        pieces = ['TSRANGEBYTIME', name]
        if tm_low is None or (isinstance(tm_low, float) and isinf(tm_low)):
            pieces.append(m_inf)
        elif time_cast_func is None:
            pieces.append(tm_low)
        else:
            pieces.append(time_cast_func(tm_low))
        if tm_high is None or (isinstance(tm_high, float) and isinf(tm_high)):
            pieces.append(p_inf)
        elif time_cast_func is None:
            pieces.append(tm_high)
        else:
            pieces.append(time_cast_func(tm_high))
        if timestamp_cast_func is None:
            return self.execute_command(*pieces, timestamp_cast_func=int)
        return self.execute_command(*pieces,
                                    timestamp_cast_func=timestamp_cast_func)

    def tsrevrangebytime(self, name, tm_low=None, tm_high=None, num=None,
                         time_cast_func=DatetimeToTimestamp(pytz.utc),
                         timestamp_cast_func=TimestampToDatetime(pytz.utc)):
        """
        Returns the entries for the given time range
        from ``tm_low`` to ``tm_high`` in the time series
        that is specified by the given key ``name``.

        The entries are returned in descending order of the times.

        Special bounds -inf (``tm_low`` is None or -inf) and
        +inf (``tm_high`` is None or inf) are also supported to retrieve
        an entire range.

        If ``num`` is specified, then at most ``num`` entries will be fetched.

        ``time_cast_func`` a callable used to cast the time ``tm``
        to a timestamp - 64 bit signed integer (cf. ``tsadd``).

        ``timestamp_cast_func`` a callable used to cast the timestamp
        return values. It should reflect how timestamp were inserted
        (cf. ``time_cast_func``).
        """
        pieces = ['TSREVRANGEBYTIME', name]
        if tm_low is None or (isinstance(tm_low, float) and isinf(tm_low)):
            pieces.append(m_inf)
        elif time_cast_func is None:
            pieces.append(tm_low)
        else:
            pieces.append(time_cast_func(tm_low))
        if tm_high is None or (isinstance(tm_high, float) and isinf(tm_high)):
            pieces.append(p_inf)
        elif time_cast_func is None:
            pieces.append(tm_high)
        else:
            pieces.append(time_cast_func(tm_high))
        if num is not None:
            pieces.extend([Token.get_token('LIMIT'), num])
        if timestamp_cast_func is None:
            return self.execute_command(*pieces, timestamp_cast_func=int)
        return self.execute_command(*pieces,
                                    timestamp_cast_func=timestamp_cast_func)
