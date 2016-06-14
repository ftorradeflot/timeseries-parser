# --------------------------------------------------------------------
# Author: Francesc Torradeflot - <ciscu@nomorecode.com>
#
# Description:
# Common functions used in the analysis module.
#
# --------------------------------------------------------------------
# Copyright (c) 2014 - All Rights Reserved.
#
# This source is subject to the Nomorecode Source License.
# Please see the License.md file for more information, which is
# part of this source code package.
# --------------------------------------------------------------------

# --------------------------------------------------------------------
# Imports and defines.
import sys
sys.path.append('../')
import pytz
from datetime import datetime, timedelta
import time
import calendar
import json
import re
import pandas as pd
import numpy as np

from common.constants import TimeInSeconds


def get_column_range(data, tz_name = 'Europe/Madrid'):
    ''' Given a dictionary with the same format as the parsed data 
        received in the get_variables function of the API, 
        arrange it to call column_range function and return its response.

    .. arguments:
    - (dict) data : dictionary with the parameters of the get_variables function
    - (tz_name) tz_name: name of the timezone of the locations we are working with

    .. returns:
    - (dict) dictionary to be partitioned as arguments of the Pycassa get function

    '''

    # data parameters
    dataParams = {}
    for param in data:
        if re.match('data\.',param):
            var = param[len('data.'):]
            dataParams[var] = data[param]

    if not dataParams:
        return {'error': json.dumps({'error': 'Invalid data range defined'})}

    columns = column_range(dataParams)

    return columns


def column_range(params, tz_name = 'Europe/Madrid', now = None, int_type = 'left_open'):
    """ Column range for cassandra's get data function #{{{
    """

    if now == None:
        now = int(time.time())
    
    if int_type == 'closed':
        exc_l = 0
        exc_r = 0
    elif int_type == 'right_open':
        exc_l = 0
        exc_r = 1
    elif int_type == 'left_open':
        exc_l = 1
        exc_r = 0
    elif int_type == 'open':
        exc_l = 1
        exc_r = 1
    else:
        return {'error': 'Unknown interval type: %s' %int_type}

    # qCount parameter may be used in combination with range, to and from parameters
    qCount = params.get('count', np.inf)

    qRange = params.get('range')
    if qRange == 'last_one':
        return {'column_start':('timeseries',now),'column_count': np.minimum(1, qCount)}

    elif qRange == 'last_hour':
        return {'column_start':('timeseries', now),
                'column_finish': ('timeseries',now - TimeInSeconds.HOUR),
                'column_count': np.minimum(TimeInSeconds.HOUR, qCount)}
    elif qRange == 'last_day':
        return {'column_start':('timeseries', now),
                'column_finish': ('timeseries',now - TimeInSeconds.DAY),
                'column_count': np.minimum(TimeInSeconds.DAY, qCount)} # 
    elif qRange == 'last_week':
        return {'column_start':('timeseries', now),
                'column_finish': ('timeseries',now - TimeInSeconds.WEEK),
                'column_count': np.minimum(TimeInSeconds.WEEK, qCount)}
    elif qRange == 'last_month':
        return {'column_start':('timeseries', now),
                'column_finish': ('timeseries',now - TimeInSeconds.MONTH),
                'column_count': np.minimum(TimeInSeconds.MONTH, qCount)}
    elif qRange == 'last_year':
        return {'column_start':('timeseries', now),
                'column_finish': ('timeseries',now - TimeInSeconds.YEAR),
                'column_count': np.minimum(TimeInSeconds.YEAR, qCount)}
    elif qRange == 'this_hour':
        return {'column_start':('timeseries', time_interval_end('hour', tz_name, now) - exc_r),
                'column_finish': ('timeseries', time_interval_beginning('hour', tz_name, now) + exc_l),
                'column_count': np.minimum(TimeInSeconds.HOUR, qCount)}
    elif qRange == 'today':
        return {'column_start':('timeseries', time_interval_end('day', tz_name, now) - exc_r),
                'column_finish': ('timeseries', time_interval_beginning('day', tz_name, now) + exc_l),
                'column_count': np.minimum(TimeInSeconds.DAY, qCount)}
    elif qRange == 'this_week':
        return {'column_start':('timeseries', time_interval_end('week', tz_name, now) - exc_r),
                'column_finish': ('timeseries', time_interval_beginning('week', tz_name, now) + exc_l),
                'column_count': np.minimum(TimeInSeconds.WEEK, qCount)}
    elif qRange == 'this_month':
        return {'column_start':('timeseries', time_interval_end('month', tz_name, now) - exc_r),
                'column_finish': ('timeseries', time_interval_beginning('month', tz_name, now) + exc_l),
                'column_count': np.minimum(TimeInSeconds.MONTH, qCount)}
    elif qRange == 'this_year':
        return {'column_start':('timeseries',  time_interval_end('month', tz_name, now) - exc_r),
                'column_finish': ('timeseries', time_interval_beginning('year', tz_name, now) + exc_l),
                'column_count': np.minimum(TimeInSeconds.YEAR, qCount)}
    elif qRange:
        return {'error': 'unknown parameter range: {!s}'.format(qRange)}

    qFrom = params.get('from')
    qTo = params.get('to')

    if qFrom and qTo:
        qFrom = int(qFrom) + exc_l
        qTo = int(qTo) - exc_r
        return {'column_start':('timeseries',qTo),
                'column_finish':('timeseries',qFrom),
                'column_count': np.minimum(TimeInSeconds.YEAR, qCount)}
    if qFrom:
        qFrom = int(qFrom) + exc_l
        return {'column_start':('timeseries', now),
                'column_finish':('timeseries',qFrom),
                'column_count': np.minimum(TimeInSeconds.YEAR, qCount)}

    # if only qTo given, return a week of data
    if qTo: 
        qTo = int(qTo) - exc_r
        return {'column_start':('timeseries',qTo),
                'column_finish': ('timeseries',qTo - TimeInSeconds.WEEK),
                'column_count': np.minimum(TimeInSeconds.WEEK, qCount)}

    # if only qCount informed return last qCount values
    if not np.isinf(qCount):
        return {'column_start':('timeseries', now), 'column_count': int(qCount)}

    # if not qFrom qTo qRange qCount, return last_one
    return {'column_start':('timeseries', now),'column_count': 1}
    #}}}




# --------------------------------------------------------------------
# Return the epoch corresponding to the beginning of the time interval
# where the epoch is located
#
# .. arguments:
#   - (string) time_int : 'year', 'month', 'week' or 'day'
#   - (string) tz_name: name of the timezone of the locations we are working with
#   - (integer) epoch_ref: epoch in utc, that references the time interval
#       of which we want to get the beginning
#
# .. returns:
#   - (integer) epoch representing the beginning of the time interval time_int
#       of moment epoch_ref in timezone tz_name
#
# TODO: The daylight saving changing date is taken from pytz module.
# ..A better approach would be to get it from the official site
# ..and use it instead of pytz.
def time_interval_beginning(time_int, tz_name = 'Europe/Madrid', epoch_ref = None):

    # set epoch_ref
    if epoch_ref == None:
        epoch_ref = int(time.time())

    # truncate epoch to minutes. So we consider the first minute of one period as
    # part of the previous one
    epoch_ref = 60*int(epoch_ref/60)

    # Get timezone object
    if tz_name not in pytz.all_timezones:
        return {'error': json.dumps({'error': 'Invalid timezone given'})}
    t_zone = pytz.timezone(tz_name)

    # Get datetime used as reference and localize it in the given timezone
    try:
        dt_r = datetime.fromtimestamp(epoch_ref, t_zone)
    except:
        return {'error': json.dumps({'error': 'Invalid datetime given'})}

    # Get datetime of the beginning of the time_interval
    '''y = dt_r.year
    m = dt_r.month
    d = dt_r.day
    wd = dt_r.weekday()
    h = dt_r.hour
    mt = dt_r.minute'''
    dtt = dt_tuple(dt_r)
    #print dtt.as_dict()
    dtt.shift()
    #print dtt.as_dict()
    if time_int == 'year':
        dt = t_zone.localize(datetime(dtt.y, 1, 1))
    elif time_int == 'month':
        dt = t_zone.localize(datetime(dtt.y, dtt.m, 1))
    elif time_int == 'week':
        dt = t_zone.localize(datetime(dtt.y, dtt.m, dtt.d)) - timedelta(dtt.w)
    elif time_int == 'day':
        dt = t_zone.localize(datetime(dtt.y, dtt.m, dtt.d))
    elif time_int == 'hour':
        #dt = t_zone.localize(datetime(y, m, d, h))
        if epoch_ref % 3600 == 0:
            return 3600*(int(epoch_ref/3600) - 1)
        else:
            return 3600*(int(epoch_ref/3600))
    else:
        return {'error': json.dumps({'error': 'Invalid time interval given: %s' %str(time_int)})}

    # Get offset of the t_zone in that moment of time
    offset_string = dt.strftime("%z")
    offset = int(offset_string[1:3]) * 3600 + int(offset_string[3:5])*60
    if offset_string[0] == '-': offset *= -1

    # return the epoch with the offset
    return calendar.timegm(dt.timetuple()) - offset


def from_epoch_obtain_ymwdh(epoch, ymwdh, tz_name = 'Europe/Madrid', shift = True):

    # Get timezone object
    if tz_name not in pytz.all_timezones:
        return {'error': json.dumps({'error': 'Invalid timezone given'})}
    t_zone = pytz.timezone(tz_name)

    # Get datetime used as reference and localize it in the given timezone
    try:
        dt_r = datetime.fromtimestamp(epoch, t_zone)
    except:
        return {'error': json.dumps({'error': 'Invalid datetime given'})}
    
    dt = dt_tuple(dt_r)
    if shift: dt.shift()

    return dt.as_dict()[ymwdh]


class dt_tuple:

    def __init__(self, dt):
        self.y = dt.year
        self.m = dt.month
        self.w = dt.weekday()
        self.d = dt.day
        self.h = dt.hour
        self.mt = dt.minute

    def as_list(self):
        return [self.y, self.m, self.w, self.d, self.h, self.mt]

    def as_dict(self):
        d = {}
        d['y'] = self.y
        d['m'] = self.m
        d['w'] = self.w
        d['d'] = self.d
        d['h'] = self.h
        d['mt'] = self.mt
        return d

    def __repr__(self):
        return str(self.as_list())

    def shift(self):
        if self.mt == 0:
            self.mt = 60
            if self.h != 0:
                self.h -= 1
            else:
                self.h = 23
                if self.w != 0:
                    self.w -= 1
                else:
                    self.w = 6
                
                if self.d != 1:
                    self.d -= 1
                else:
                    new_dt = datetime(self.y, self.m, self.d) - timedelta(1)
                    self.d = new_dt.day

                    if self.m != 1:
                        self.m -= 1
                    else:
                        self.m = 12
                        self.y -= 1                        
                        

def check_expected_values(df, column, expected_values):

    grouped_df = pd.DataFrame(df.groupby(column).count())

    arr_values = grouped_df.index.values

    return np.array_equal(arr_values, expected_values)


def time_interval_end(time_int, tz_name = 'Europe/Madrid', epoch_ref = None):

    ''' Return the epoch corresponding to the end of the time interval
         where epoch_ref is located

        .. arguments:
            - (string) time_int : 'year', 'month', 'week' or 'day'
            - (string) tz_name: name of the timezone of the locations we are working with
            - (integer) epoch_ref: epoch in utc, that references the time interval
                of which we want to get the beginning

        .. returns:
            - (integer) epoch representing the beginning of the time interval time_int
                of moment epoch_ref in timezone tz_name

        TODO: The daylight saving changing date is taken from pytz module.
        ..A better approach would be to get it from the official site
        ..and use it instead of pytz.'''


    # set epoch_ref
    if epoch_ref == None:
        epoch_ref = time.time()

    # truncate epoch to minutes. So we consider the first minute of one period as
    # part of the previous one
    epoch_ref = 60*int(epoch_ref/60)

    # Get timezone object
    if tz_name not in pytz.all_timezones:
        return {'error': json.dumps({'error': 'Invalid timezone given'})}
    t_zone = pytz.timezone(tz_name)

    # Get datetime used as reference and localize it in the given timezone
    try:
        dt_r = datetime.fromtimestamp(epoch_ref, t_zone)
    except:
        return {'error': json.dumps({'error': 'Invalid datetime given'})}

    # Get datetime of the end of the time_interval
    dtt = dt_tuple(dt_r)
    #print dtt.as_dict()
    dtt.shift()
    #print dtt.as_dict()

    if time_int == 'year':
        y = dtt.y + 1
        dt = t_zone.localize(datetime(y, 1, 1))
    elif time_int == 'month':
        if dtt.m == 12:
            m = 1
            y = dtt.y + 1
        else:
            m = dtt.m + 1
            y = dtt.y
        dt = t_zone.localize(datetime(y, m, 1))
    elif time_int == 'week':
        wd = 7 - dtt.w
        dt = t_zone.localize(datetime(dtt.y, dtt.m, dtt.d)) + timedelta(wd)
    elif time_int == 'day':
        dt = t_zone.localize(datetime(dtt.y, dtt.m, dtt.d)) + timedelta(1)
    elif time_int == 'hour':
        #dt = t_zone.localize(datetime(y, m, d, h)) + timedelta(hours = 1)
        if epoch_ref % 3600 == 0:
            return 3600*(int(epoch_ref/3600))
        else:
            return 3600*(int(epoch_ref/3600) + 1)
    else:
        return {'error': json.dumps({'error': 'Invalid time interval given: %s' %str(time_int)})}

    # Get offset of the t_zone in that moment of time
    offset_string = dt.strftime("%z")
    offset = int(offset_string[1:3]) * 3600 + int(offset_string[3:5])*60
    if offset_string[0] == '-': offset *= -1

    # return the epoch with the offset
    return calendar.timegm(dt.timetuple()) - offset



def rearrange_timeseries(oDict):
#{{{
    l = []
    for key in oDict:
        (mode, clock) = key
        data = json.loads(oDict[key])
        try:
            if not 'value' in data:
                continue
            l.append((clock,data['value']))
        except TypeError:
            print "An ERROR occurred while rearranging timeseries - data.py"
    l.reverse()
    return l
    #}}}

