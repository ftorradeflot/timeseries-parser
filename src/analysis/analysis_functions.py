# --------------------------------------------------------------------
# Author: Francesc Torradeflot - <ciscu@nomorecode.com>
#
# Description:
# Time series analysis functions
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
import re
from pycassa import ColumnFamily, NotFoundException
import json
import numpy as np

from orm.cassandra_util import load_pool
import orm.sqlalchemy_model as sqlm
from analysis_utils import rearrange_timeseries
from analysis_utils import time_interval_beginning as tib

def json_error_not_exists(Item, attr=None, attrValue=None):
    """ Item does not exist.
    """
    if attrValue == None:
        return json.dumps({'error':'no ' + Item.__name__ + ' with attributes given'})
    return json.dumps({'error':'no ' + Item.__name__ + ' with ' + str(attr) + '=' + str(attrValue)})

# -------------------------------------------------------------------------
# Return a list containing a list with the required data of a given variable.
# It is basically the same function as get_variables of the api, but 
# the output is just a timeseries
#
# .. arguments:
#   - (string) id_variable: id of the variable we want to retrieve the data from
#   - (dict) columns: contains the time range of data we want to receive
#
# .. returns:
#   - on success: list containing a list with the timeserie. This data formatting is
#       assumed with the aim of mantaing the coherency with the rest of the 
#       timeseries functions
#       [[(epoch_0, value_0), ..., (epoch_n, value_n)]] from older to newer
#   - on error:
#       - variable does not exist error
#       - invalid data_range given
#       - no data found
#
def get_variable_data(id_variable, columns):

    # Load Postgres session
    session = sqlm.load_session()

    # get variable by id
    variable = session.query(sqlm.Variable).\
            filter(sqlm.Variable.id == id_variable).\
            filter(sqlm.Variable.deletion_date == None).\
            first()

    if not variable:
        session.close()
        return {'error': json_error_not_exists(sqlm.Variable)}

    # build query cassandra, structured by tables
    query_cassandra = {} # {table name: list of cassandra ids}

    # query cassandra
    pool = load_pool()
    pool.timeout = 300

    cf = ColumnFamily(pool, variable.timeseries_cassandra)

    try:
        ans_cassandra = cf.get(variable.id_cassandra, **columns)
    except NotFoundException:
        session.close()
        pool.dispose()
        return {'error': json.dumps({'error': 'No data found'})}

    output = rearrange_timeseries(ans_cassandra)

    session.close()
    pool.dispose()

    # the output is returned as a list of one list
    return [output]


# -------------------------------------------------------------------------
# Return a list containing all the given timeseries divided in shorter timeseries
# of length the specified timespan
#
# .. arguments:
#   - (list) time_series: list of time_series
#       [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]
#   - (string) time_int : the time interval for which we want to divide 
#       the timeseries. Accepted values are: 'year', 'month', 'week' or 'day'       
#   - (list) data_range: contains the time range of data
#       we want to receive: [epoch_from , epoch_to]
#   - (string) tz_name: name of the timezone of the locations we are working with
#
# .. returns:
#   - on success: list containing a list with the timeseries divided into smaller ones of 
#       length time_int
#   - on error: NO ERRORS HANDLED
#
def timeseries_group_by(time_series, time_int, data_range = None, tz_name = 'Europe/Madrid'):

    output = []

    for ts in time_series:

        if data_range != None:
            [e_1, e_2] = data_range
        else:
            e_1 = ts[0][0]
            e_2 = ts[-1][0]

        epoch_pivot = tib(time_int, tz_name = tz_name, epoch_ref = e_2)
        old_epoch_pivot = epoch_pivot
        old_epoch_pivot = e_2
        pointer = len(ts) - 1
        grouped_ts = []
        divided_ts = []
        count = 0

        while old_epoch_pivot > e_1:
            # point strictly of the interior of an interval
            if pointer > 0 and ts[pointer][0] >= epoch_pivot and ts[pointer - 1][0] >= epoch_pivot:
                divided_ts.insert(0, ts[pointer])
                pointer -= 1
            # outer point but not on the border
            elif pointer > 0 and ts[pointer][0] > epoch_pivot and ts[pointer - 1][0] < epoch_pivot:
                divided_ts.insert(0, ts[pointer])
                divided_ts.insert(0, [epoch_pivot, ts[pointer][1]])
                grouped_ts.insert(0, divided_ts)

                divided_ts = []
                divided_ts.insert(0, [epoch_pivot, ts[pointer][1]])                                
                pointer -= 1
                old_epoch_pivot = epoch_pivot
                epoch_pivot -= 1
                epoch_pivot = tib(time_int, tz_name = tz_name, epoch_ref = epoch_pivot)
            # point on the border
            elif pointer > 0 and ts[pointer][0] == epoch_pivot and ts[pointer - 1][0] < epoch_pivot:
                divided_ts.insert(0, ts[pointer])
                grouped_ts.insert(0, divided_ts)

                divided_ts = []
                divided_ts.insert(0, ts[pointer])                                
                pointer -= 1
                old_epoch_pivot = epoch_pivot
                epoch_pivot -= 1
                epoch_pivot = tib(time_int, tz_name = tz_name, epoch_ref = epoch_pivot)
            # last point is interior
            elif pointer == 0 and ts[pointer][0] > epoch_pivot:
                divided_ts.insert(0, ts[pointer])
                grouped_ts.insert(0, divided_ts)

                divided_ts = []                                
                pointer -= 1
                old_epoch_pivot = epoch_pivot
                epoch_pivot -= 1
                epoch_pivot = tib(time_int, tz_name = tz_name, epoch_ref = epoch_pivot)
            # last point is on the border
            elif pointer == 0 and ts[pointer][0] == epoch_pivot:
                divided_ts.insert(0, ts[pointer])
                grouped_ts.insert(0, divided_ts)

                divided_ts = []
                divided_ts.insert(0, ts[pointer])
                pointer -= 1
                old_epoch_pivot = epoch_pivot
                epoch_pivot -= 1
                epoch_pivot = tib(time_int, tz_name = tz_name, epoch_ref = epoch_pivot)
            # Else clause covers two cases:
            #   - when the pointer is greater than 0 but its time reference
            #       is lower than the epoch_pivot. This situation can happen when the upper 
            #       bound is greater than the greatest of the time refs in the timeserie
            #       or because of a gap in the data of the timeserie
            #   - when the pointer is lower than 0 but epoch_pivot has not reached
            #       the lower bound of the data_range
            else:
                if divided_ts == []:
                    divided_ts.insert(0, (max([e_1, epoch_pivot]), None))
                grouped_ts.insert(0, divided_ts)
                old_epoch_pivot = epoch_pivot
                epoch_pivot -= 1
                epoch_pivot = tib(time_int, tz_name = tz_name, epoch_ref = epoch_pivot)
                # Time savings changes can result in an infinite while
                # To prevent this, we introduce the following if
                if epoch_pivot == old_epoch_pivot and time_int == 'hour':
                    epoch_pivot -= 3600
                divided_ts = []

        output += grouped_ts

    return output

# -------------------------------------------------------------------------
# Return a list containing the number of times the state value given has 
# arised for each of the timeseries given
#
# .. arguments:
#   - (list) time_series: list of time_series
#       [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]
#   - (unknown) state_value: this is the value we want to check if it has arised
#       it can be an integer (0,1) or a string (on, off)
#
# .. returns:
#   - on success: list containing a list with the number of times the value has
#       arised in each timeserie
#
def count_state_change(time_series, state_value):

    output = []

    for ts in time_series:
        state_count = 0
        in_state = True
        epoch = ts[0][0]
        for element in ts:
            
            if str(element[1]) == str(state_value):
                if in_state:
                    pass
                else:
                    state_count += 1
                    in_state = True
            else:
                if in_state:
                    in_state = False
                else:
                    pass
        output.append((epoch, state_count))

    return [output]

# -------------------------------------------------------------------------
# Return a list containing the given timeseries with all the values
# converted to float type, if possible. If it is not possible, return error.
#
# This function should be called always before starting the execution
# of arithmetic operations with timeseries. Doing this we will
# avoid introducing data checking in all aritmethic functions
#
# .. arguments:
#   - (list) time_series: list of time_series
#       [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]
#
# .. returns:
#   - on success: list containing a list of timeseries of float values
#
def timeseries_to_float(timeseries):

    output = []

    for ts in timeseries:
        
        new_ts = []
        for elem in ts:
            (epoch, value) = elem

            if value == None:
                new_ts.append((epoch, value))                
            else:
                new_val_err = False
                try:
                    new_val = float(value)
                except:
                    new_val_err = True

                if new_val_err:
                    return {'error': json.dumps({'error': 'Invalid value %s received' %str(value)})}

                new_ts.append((epoch, new_val))

        output.append(new_ts)

    return output

# -------------------------------------------------------------------------
# Return a list containing the increments registered in each of the timeseries given
# The values of the timeseries are supposed to come from a meter
# and will always increase, except when the meter is reseted. 
# The reset value by default is 0
#
# .. arguments:
#   - (list) time_series: list of time_series
#       [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]
#   - (float) reset_value: value to which the meter is reseted
#
# .. returns:
#   - on success: list containing a list with the number of times the value has
#       arised in each timeserie. The epoch of each element will be the first
#       epoch of each timeserie.
#
def compute_meter_increments(time_series, reset_value = 0):

    output = []

    for ts in time_series:

        if len(ts) == 0:
            return {'error': json.dumps({'error': 'Invalid timeserie received'})}

        epoch = ts[0][0]
        inc = 0

        if len(ts) == 1:
            output.append((epoch, inc))
        else:                
            t0 = float(ts[0][1])
            t1 = float(ts[1][1])

            for i in range(1, len(ts) - 1):

                if t1 >= t0:
                    inc = inc + t1 - t0
                else:
                    inc = inc + t1 - reset_value

                t0 = t1
                if i < len(ts) - 1:
                    t1 = float(ts[i + 1][1])

            output.append((epoch, inc))

    return [output]

# -------------------------------------------------------------------------
# Calculate the product between the scalar given and all the timeseries
# That means not changing the length or the order of the timeseries 
# just multiply all the values for the given scalar
#
# This functions has been originaly created to apply a cost per unit
# to the increments register by a meter.
#
# .. arguments:
#   - (list) time_series: list of time_series
#       [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]
#   - (float) cost_per_unit: value we want to multiply the timeseries for
#
# .. returns:
#   - on success: list containing a list of timeseries multiplied by the given 
#       scalar
#
def scalar_product(timeseries, scalar):

    output = []

    try:
        scalar = float(scalar)
    except:
        return {'error': json.dumps({'error': 'Value received is not a number: ' + str(scalar)})}
    
    for ts in timeseries:
        
        new_ts = []
        for element in ts:
            new_ts.append((element[0], scalar*element[1]))

        output.append(new_ts)
    
    return output



def truncate_timeseries(timeseries, truncate_unit = 1):
    '''Truncate the values in a numeric timeseries

     .. arguments:
       - (list) timeseries: list of timeseries
           [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]
       - (float) truncate_unit: value to which multiples we want to truncate
           the values of the timeseries

    .. returns:
        - on success: list containing a list of timeseries truncated to the given unit'''

    # Check that truncate_unit is numeric
    try:
        truncate_unit = float(truncate_unit)
    except:
        return {'error': json.dumps({'error': 'truncate_unit is not numeric'})}

    # Check that it is a positive value
    if truncate_unit <= 0:
        return {'error': json.dumps({'error': 'truncate_unit is not positive'})}

    output = []
    for t in timeseries:
        new_ts = []
        for element in ts:
            new_ts.append((element[0], truncate_unit*int(element[1]/truncate_unit)))

        output.append(new_ts)
    
    return output


# -------------------------------------------------------------------------
# round up the values in a numeric timeseries
#
# .. arguments:
#   - (list) timeseries: list of timeseries
#       [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]
#   - (float) round_unit: value to which multiples we want to round up
#       the values of the timeseries
#
# .. returns:
#   - on success: list containing a list of timeseries rounded up to the given unit
#
def round_timeseries(timeseries, round_unit = 1):

    # Check that round_unit is numeric
    try:
        truncate_unit = float(round_unit)
    except:
        return {'error': json.dumps({'error': 'round_unit is not numeric'})}

    # Check that it is a positive value
    if round_unit <= 0:
        return {'error': json.dumps({'error': 'round_unit is not positive'})}

    output = []
    for t in timeseries:
        new_ts = []
        for element in ts:
            new_ts.append((element[0], round_unit*round(element[1]/round_unit)))

        output.append(new_ts)
    
    return output


# -----------------------------------------------------
# Calculate frecuencies of a given list of values of a variable
# that can take numeric values non_discrete range
#
# .. arguments:
#   - (list) values: list of numeric values
#   - (float) lower_limit: lower limit of the interval of data we want to get the
#       frecuencies from
#   - (float) upper_limit: upper limit of the interval of data we want to get the
#       frecuencies from
#   - (integer) n_ints: number of intervals we we will divide de data in
#
# .. returns:
#   - on success: list of frecuencies
#       [(int_0, int_1, value_0), (int_1, int_2, value_1),..., (int_n_1, int_n, int_n_1)]
#        
def non_discrete_frecuencies(values, lower_limit = 0, upper_limit = None, n_ints = 100):

    # Check the values of lower_limit
    try:
        lower_limit = float(lower_limit)
    except:
        return {'error': json.dumps({'error': 'lower_limit is not numeric'})}

    # Check the value of upper_limit or get it if it is None
    if upper_limit != None:
        try:
            upper_limit = float(upper_limit)
        except:
            return {'error': json.dumps({'error': 'upper_limit is not numeric'})}
    else:
        try:
            upper_limit = max(values)
        except:
            return {'error': json.dumps({'error': 'unable to find upper_limit'})}

    # Check that upper_limit is greater than lower_limit
    if upper_limit <= lower_limit:
        return {'error': json.dumps({'error': 'upper_limit lower than lower_limit'})}

    # Check that n_ints has the required format, integer and greater than 0
    try:
        n_ints = int(n_ints)
    except:
        return {'error': json.dumps({'error': 'number of intervals is not a positive integer'})}        

    if n_ints <= 0:
        return {'error': json.dumps({'error': 'number of intervals lower or equal than 0'})}         

    # Build results list
    l_int = float(upper_limit - lower_limit)/n_ints
    results_list = [[lower_limit + i*l_int, upper_limit - (n_ints - i - 1)*l_int, 0] for i in range(n_ints)]

    # Compute frecuencies
    for ind, v in enumerate(values):
        if v >= lower_limit and v < upper_limit:
            results_list[int((v - lower_limit)/l_int)][2] += 1

    return results_list


def distr_std_timeseries(timeseries, time_int = 900., monotony = 'increasing', reset_value = 0):

    '''Standardize a timeseries to intervals of time_int seconds
        by distributing the values by linear interpolation

    .. arguments:
    - (list) timeseries: list of timeseries
        [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]
    - (float) time_int: number of second between each value of the timeseries
    - (string) monotony: increasing / decreasing / non_monotonous

    .. returns:
    - on success: list of standardized timeseries
        [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]'''

    std_ts_list = []
 
    for ts in timeseries:
        
        new_ts = []
        if len(ts) <= 1:
            std_ts_list.append(new_ts)
            continue

        # Calculate the first standard moment after the beginning of the timeserie
        # and insert it if there is a coincidence
        epoch_pivot = int(ts[0][0]/time_int)*time_int
        if epoch_pivot != ts[0][0]:
            epoch_pivot += time_int

        end_of_ts = False

        i = 0

        while not end_of_ts:

            # If the pivot falls between two elements of the timeseries
            # insert the interpolated value
            if ts[i][0] <= epoch_pivot and ts[i + 1][0] > epoch_pivot:

                t_1 = ts[i + 1][0] - ts[i][0]
                t_2 = epoch_pivot - ts[i][0]

                # Handle the meter resets in an incremental meter
                if ts[i][1] > ts[i + 1][1] and monotony == 'increasing':
                    value = reset_value + (t_2/t_1)*(ts[i + 1][1] - reset_value)
                # Handle the meter resets in a decremental meter
                elif ts[i][1] < ts[i + 1][1] and monotony == 'decreasing':
                    value = reset_value + (t_2/t_1)*(ts[i + 1][1] - reset_value)
                else:
                    value = ts[i][1] + (t_2/t_1)*(ts[i + 1][1] - ts[i][1]) 
               
                new_ts.append((epoch_pivot, value))
                epoch_pivot += time_int

            elif epoch_pivot >= ts[i + 1][0]:

                if i == len(ts) - 2:
                    end_of_ts = True
                else:
                    i += 1
        
            #print i, epoch_pivot, ts[i][0], ts[i+1][0]
        std_ts_list.append(new_ts)

    return std_ts_list


def value_to_increments(timeseries, monotony = 'increasing', reset_value = 0.):

    '''Return a timeserie with the increments registered in the 
        input timeserie

    .. arguments:
    - (list) timeseries: list of timeseries
        [[(epoch_0_0, value_0_0),..., (epoch_0_n0, value_0_n0)], ... (n timeseries)]
    - (string) monotony: increasing / decreasing / non_monotonous
    - (float) reset_value: value to which the meter is reseted.

    .. returns:
    - on success: list of timeseries of increments. Each timeseries contains 
        one value less than the original one. The diference between 
        two values, is assigned to the epoch of the second one.
        [[(epoch_0_1, inc_0_1),..., (epoch_0_n0, inc_0_n0)], ... (n timeseries)]'''

    inc_ts_list = []

    for ts in timeseries:

        new_ts = []
        l = len(ts)
        if l <= 1:
            inc_ts_list.append(new_ts)
            continue
        else:

            for i in range(l - 1):

                # Handle the meter resets in an incremental meter
                if ts[i][1] > ts[i + 1][1] and monotony == 'increasing':
                    value = ts[i + 1][1] - reset_value
                # Handle the meter resets in a decremental meter
                elif ts[i][1] < ts[i + 1][1] and monotony == 'decreasing':
                    value = ts[i + 1][1] - reset_value
                else:
                    value = ts[i + 1][1] - ts[i][1]
 
                new_ts.append([ts[i + 1][0], value])

        inc_ts_list.append(new_ts)

    return inc_ts_list


def clean_duplicated(timeseries):

    clean_ts_list = []

    for ts in timeseries:
    
        new_ts = []

        l = len(ts)

        if l == 0:
            clean_ts_list.append(new_ts)
            continue
        elif l == 1:
            new_ts.append((ts[0][0], ts[0][1]))
            clean_ts_list.append(new_ts)
            continue
        else:

            pivot = 0
            pivot_elem = ts[pivot]
            new_ts.append(pivot_elem)
            
            for i in range(1, l):
                if pivot_elem[1] != ts[i][1]:
                    pivot = i
                    pivot_elem = ts[i]
                    new_ts.append(ts[i])

        clean_ts_list.append(new_ts)

    return clean_ts_list


def delete_critical_values(timeseries, critical_value):

    deleted_ts_list = []

    for ts in timeseries:

        new_ts = []

        for elem in ts:
            if elem[1] != critical_value:
                new_ts.append(elem)

        deleted_ts_list.append(new_ts)

    return deleted_ts_list

def gaussian(x,amp=1,mean=0,sigma=1):
        return amp*np.exp(-(x-mean)**2/(2*sigma**2))

def gaussian_smooth(frec, p_width = 3):

    gaussian_smooth = []
    inc = frec[0][1] - frec[0][0]
    width = inc * p_width
    x = np.array([(float(elem[1]) + float(elem[0]))/2 for elem in frec])
    y = np.array([elem[2] for elem in frec])

    for ind, elem in enumerate(frec):
        gaussian_center = x[ind]
        weights = gaussian(x, mean = gaussian_center, sigma = width)
        w_avg = np.average(y, weights = weights)
        gaussian_smooth.append([frec[ind][0], frec[ind][1], w_avg])

    return gaussian_smooth

