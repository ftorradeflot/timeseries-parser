# --------------------------------------------------------------------
# Author: Francesc Torradeflot - <ciscu@nomorecode.com>
#
# Description:
# Tests on analysis/timeseries_utils.py
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
from nose.tools import *
import sys
sys.path.append('../../src')
import json

from analysis.timeseries_functions import *
import pandas as pd
from pandas.util.testing import assert_frame_equal

# Fake objects created for testing

TS_1 = [[(1356994800, 1), #1/1/2013 0:0:0
(1388530800, 0),  #1/1/2014 0:0:0
(1391209200, 1), #1/2/2014 0:0:0
(1391295600, 0), #2/2/2014 0:0:0
(1391986800, 1), #10/2/2013 0:0:0
(1392073200, 0),  #11/2/2014 0:0:0
(1393282800, 1), #25/2/2014 0:0:0
(1393628400, 0)]] #1/3/2014 0:0:0

TS_2 = [[(1388530800, 0),  #1/1/2014 0:0:0
(1391209200, 1), #1/2/2014 0:0:0
(1391295600, 0)], #2/2/2014 0:0:0
[(1391986800, 1), #10/2/2013 0:0:0
(1392073200, 0),  #11/2/2014 0:0:0
(1393282800, 1), #25/2/2014 0:0:0
(1393628400, 0)]] #1/3/2014 0:0:0


@nottest
def test_ts_list_equality(ts_1, ts_2):

    assert_equal(len(ts_1), len(ts_2))
    for ind in range(len(ts_1)):
        assert_frame_equal(ts_1[ind], ts_2[ind])

# --------------------------------------------------------------------
# check_ts
def test_cts_1():

    argument = [(1356994800, 1), (1356994800, 1)]
    expected_output = {'error': 'Element is not a timeserie: DataFrame expected'}

    real_output = check_ts(argument)

    assert_equal(expected_output, real_output)


def test_cts_2():

    argument = pd.DataFrame([0, 1], columns = ['value'], index = ['a', 1393628690])
    expected_output = {'error': 'Element is not a timeserie: Integer index required'}

    real_output = check_ts(argument)

    assert_equal(expected_output, real_output)


def test_cts_3():

    argument = pd.DataFrame([0, 1], columns = ['value'], index = [-1, 1393628690])
    expected_output = {'error': 'Element is not a timeserie: Non positive values in index'}

    real_output = check_ts(argument)

    assert_equal(expected_output, real_output)


def test_cts_4():

    argument = pd.DataFrame([0, 0, 0, 1, 1, 1, 1], columns = ['value'], dtype = 'float64', \
        index = [1393628400 + 100* i for i in range(7)])
    expected_output = {'success': 1}

    real_output = check_ts(argument)

    assert_equal(expected_output, real_output)


def test_cts_5():

    argument = pd.DataFrame([[0, 0, 0, 1, 1, 1, 1],[0, 0, 0, 1, 1, 1, 1]])
    expected_output = {'error': 'Element is not a timeseries: One column required'}

    real_output = check_ts(argument)

    assert_equal(expected_output, real_output)


def test_cts_6():

    argument = pd.DataFrame([0, 0, 0, 1, 1, 1, 1], columns = ['test'])
    expected_output = {'error': 'Element is not a timeseries: value column required'}

    real_output = check_ts(argument)

    assert_equal(expected_output, real_output)

# --------------------------------------------------------------------
# cassandra_to_ts_list
def test_cttl():
    result = [pd.DataFrame([1, 0, 1, 0, 1, 0, 1, 0], columns = ['value'], index = [1356994800, 1388530800,\
        1391209200, 1391295600, 1391986800, 1392073200, 1393282800, 1393628400])]

    ts_2 = cassandra_to_ts_list(TS_1)

    test_ts_list_equality(ts_2, result)

# --------------------------------------------------------------------
# distribute_ts_list
def test_dttsl_1():

    argument = [pd.DataFrame([0, 1], columns = ['value'], index = [1393628450, 1393628690], dtype = 'float64')]
    expected_output = [pd.DataFrame([1], columns = ['value'], index = [1393628700], dtype = 'float64')]

    real_output = distribute_ts_list(argument)

    test_ts_list_equality(real_output, expected_output)


def test_dttsl_2():

    argument = [pd.DataFrame([0, 1], columns = ['value'], dtype = 'float64', index = [1393628450, 1393628690])]
    expected_output = [pd.DataFrame([0, 0, 1, 1, 1, 1, 1], columns = ['value'], dtype = 'float64', \
        index = [1393628500 + 100* i for i in range(7)])]

    real_output = distribute_ts_list(argument, seconds = 100, e_from = 1393628430, e_to =  1393629010)

    test_ts_list_equality(real_output, expected_output)


def test_dttsl_3():

    argument = [pd.DataFrame([0, 1, 1], columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900])]
    expected_output = [pd.DataFrame([0, 0, 0, 0, 1, 1, 1], columns = ['value'], \
        index = [1393627200 + 300* i for i in range(7)], dtype = 'float64')]

    real_output = distribute_ts_list(argument, e_from = 1393627000)

    test_ts_list_equality(real_output, expected_output)


def test_dttsl_4():

    argument = [pd.DataFrame([i*500 for i in range(5)] + [i*100 for i in range(5)],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000])]
    expected_output = [pd.DataFrame([0,500,500,1000,1000,2000,0,100,200,300,400],
        columns = ['value'], index = [i for i in range(1393628100, 1393631400, 300)], 
        dtype = 'float64')]

    real_output = distribute_ts_list(argument)

    print expected_output, real_output

    test_ts_list_equality(real_output, expected_output)

# --------------------------------------------------------------------
# increments
def test_inc_1():

    argument = [pd.DataFrame([i*500 for i in range(5)] + [i*100 for i in range(5)],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000])]
    expected_output = [pd.DataFrame([500, 500, 500, 500, 0, 100, 100, 100, 100], columns = ['value'], \
        index = [1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000], dtype = 'float64')]

    real_output = increments(argument)

    test_ts_list_equality(real_output, expected_output)


def test_inc_2():

    argument = {'error': 'Test error'}
    expected_output = {'error': 'Test error'}

    real_output = increments(argument)

    assert_equal(real_output, expected_output)


def test_inc_3():

    argument = 'a'
    expected_output = {'error': 'Not a list of timeseries'}

    real_output = increments(argument)

    assert_equal(real_output, expected_output)


def test_inc_4():

    argument = [pd.DataFrame([1, 10, 20, 30, 0, -10, -20, 0, -15, -25],\
        columns = ['value'], index = [i for i in range(1, 11)])]
    expected_output = {'error': 'value greater than reset value'}

    real_output = increments(argument, monotony = 'decreasing')

    assert_equal(real_output, expected_output)


def test_inc_5():

    argument = [pd.DataFrame(['a', 10, 20, 30, 0, -10, -20, 0, -15, -25],\
        columns = ['value'], index = [i for i in range(1, 11)])]
    expected_output = {'error': 'Non scalar values found'}

    real_output = increments(argument)

    #print expected_output
    #print real_output

    assert_equal(real_output, expected_output)


def test_inc_6():

    argument = [pd.DataFrame([1, 10, 20, 30, 0, -10, -20, 0, -15, -25],\
        columns = ['value'], index = [i for i in range(1, 11)])]
    expected_output = {'error': 'reset_value is not a number'}

    real_output = increments(argument, reset_value = 'a')

    assert_equal(real_output, expected_output)


def test_inc_7():

    argument = [pd.DataFrame([1, 10, 20, 30, 0, -10, -20, 0, -15, -25],\
        columns = ['value'], index = [i for i in range(1, 11)])]
    expected_output = {'error': 'max_value is not a number'}

    real_output = increments(argument, max_value = 'a')

    assert_equal(real_output, expected_output)


def test_inc_8():

    argument = [pd.DataFrame([1, 10, 20, 30, 0, 15, 30, 50, 2, 5],\
        columns = ['value'], index = [i for i in range(1, 11)])]
    expected_output = [pd.DataFrame([9, 10, 10, 20, 15, 15, 20, 2, 3], 
        columns = ['value'], index = [i for i in range(2, 11)], dtype = 'float64')]

    real_output = increments(argument, max_value = 50)

    test_ts_list_equality(real_output, expected_output)


def test_inc_9():

    argument = [pd.DataFrame([1, 10, 20, 30, 0, 15, 30, 50, 2, 5],\
        columns = ['value'], index = [i for i in range(1, 11)])]
    expected_output = {'error': 'value lower than reset_value'}

    real_output = increments(argument, max_value = 50, reset_value = 5)

    assert_equal(real_output, expected_output)


def test_inc_10():

    argument = [pd.DataFrame([1, 10, 20, 30, 0, 15, 30, 50, 2, 5],\
        columns = ['value'], index = [i for i in range(1, 11)])]
    expected_output = [pd.DataFrame([-41, -40, -40, -30, -35, -35, -30, -48, -47], 
        columns = ['value'], index = [i for i in range(2, 11)], dtype = 'float64')]

    real_output = increments(argument, monotony = 'decreasing', max_value = 0, reset_value = 50)

    test_ts_list_equality(real_output, expected_output)


def test_inc_11():

    argument = [pd.DataFrame([1, 10, 20, 0, -15, 100, 30, 50, 2, 5],\
        columns = ['value'], index = [i for i in range(1, 11)])]
    expected_output = [pd.DataFrame([9, 10, -20, -15, 115, -70, 20, -48, 3], 
        columns = ['value'], index = [i for i in range(2, 11)], dtype = 'float64')]

    real_output = increments(argument, monotony = 'non-monotonous', max_value = 0, reset_value = 50)

    test_ts_list_equality(real_output, expected_output)

# ------------------------------ Functions with numbers ----------------------------------
# --------------------------------------------------------------------
# scalar_product
def test_scp_1():

    argument = [pd.DataFrame([500 for i in range(5)] + [100 for i in range(5)],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000])]
    expected_output = [pd.DataFrame([1000, 1000, 1000, 1000, 1000, 200, 200, 200, 200, 200], \
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000], dtype = 'float64')]

    real_output = scalar_product(argument, number = 2.)

    test_ts_list_equality(real_output, expected_output)


def test_scp_2():

    argument = [pd.DataFrame(['a', 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500])]
    expected_output = {'error': 'Non scalar values found'}

    real_output = scalar_product(argument)

    assert_equal(real_output, expected_output)


def test_scp_3():

    argument = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500])]
    expected_output = {'error': 'number is not numeric'}

    real_output = scalar_product(argument, number = 'a')

    assert_equal(real_output, expected_output)


def test_scp_4():

    argument = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500])]
    expected_output = {'error': 'unknown argument test'}

    real_output = scalar_product(argument, test = 1)

    assert_equal(real_output, expected_output)


def test_scp_5():

    argument = [pd.DataFrame([-1 for i in range(10)],\
        columns = ['value'], dtype= 'float64', \
        index = [1 for i in range(10)])]
    expected_output = {'error': 'Non unique index'}

    real_output = scalar_product(argument, number = -5)

    assert_equal(real_output, expected_output)


# --------------------------------------------------------------------------------------------
# scalar_division
def test_scdiv_1():

    argument = [pd.DataFrame([500 for i in range(5)] + [100 for i in range(5)],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000])]
    expected_output = [pd.DataFrame([250 for i in range(5)] + [50 for i in range(5)], \
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000], dtype = 'float64')]

    real_output = scalar_division(argument, number = 2.)

    test_ts_list_equality(real_output, expected_output)


def test_scdiv_2():

    argument = [pd.DataFrame(['a', 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500])]
    expected_output = {'error': 'Non scalar values found'}

    real_output = scalar_division(argument)

    assert_equal(real_output, expected_output)


def test_scdiv_3():

    argument = [pd.DataFrame([-1 for i in range(10)],\
        columns = ['value'], dtype= 'float64', \
        index = [i for i in range(10)])]
    expected_output = [pd.DataFrame([1./5. for i in range(10)],\
        columns = ['value'], dtype= 'float64', \
        index = [i for i in range(10)])]

    real_output = scalar_division(argument, number = -5)

    test_ts_list_equality(real_output, expected_output)



# --------------------------------------------------------------------------------------------
# scalar_subtraction
def test_scsub_1():

    argument = [pd.DataFrame([500 for i in range(5)] + [100 for i in range(5)],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000])]
    expected_output = [pd.DataFrame([400 for i in range(5)] + [0 for i in range(5)], \
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000], dtype = 'float64')]

    real_output = scalar_sub(argument, number = 100.)

    test_ts_list_equality(real_output, expected_output)


def test_scsub_2():

    argument = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500], dtype= 'float64')]
    expected_output = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500], dtype= 'float64')]

    real_output = scalar_sub(argument)

    test_ts_list_equality(real_output, expected_output)


def test_scsub_3():

    argument = [pd.DataFrame([-1 for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)]), 
        pd.DataFrame([-i for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)])]
    expected_output = [pd.DataFrame([4 for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)]),
        pd.DataFrame([5 - i for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)])]

    real_output = scalar_sub(argument, number = -5)

    test_ts_list_equality(real_output, expected_output)


# --------------------------------------------------------------------------------------------
# scalar_power
def test_scpow_1():

    argument = [pd.DataFrame([2 for i in range(5)] + [3 for i in range(5)],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000])]
    expected_output = [pd.DataFrame([4 for i in range(5)] + [9 for i in range(5)], \
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000], dtype = 'float64')]

    real_output = scalar_power(argument, number = 2.)

    test_ts_list_equality(real_output, expected_output)


def test_scpow_2():

    argument = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500], dtype= 'float64')]
    expected_output = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500], dtype= 'float64')]

    real_output = scalar_power(argument)

    test_ts_list_equality(real_output, expected_output)


def test_scpow_3():

    argument = [pd.DataFrame([2 for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)]), 
        pd.DataFrame([-1 for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)])]
    expected_output = [pd.DataFrame([0.25 for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)]),
        pd.DataFrame([1 for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)])]

    real_output = scalar_power(argument, number = -2.5)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


# ---------------------------------- Aggregate functions ----------------------------------------
# --------------------------------------------------------------------
# inner_sum
def test_is_1():

    argument = [pd.DataFrame([1 for i in range(10)], columns = ['value'], \
        dtype= 'float64', index = [i for i in range(10)]), 
        pd.DataFrame([3 for i in range(5)], columns =  ['value'],\
        dtype= 'float64', index = [i for i in range(10, 15)])]
    expected_output = [pd.DataFrame([10, 15], columns = ['value'], \
        dtype= 'float64', index = [9, 14])]

    real_output = inner_sum(argument)

    test_ts_list_equality(real_output, expected_output)


def test_is_2():

    argument = [pd.DataFrame([1 for i in range(10)], columns = ['value'], \
        dtype= 'float64', index = [1 for i in range(10)]), 
        pd.DataFrame([3 for i in range(5)], columns =  ['value'],\
        dtype= 'float64', index = [i for i in range(5)])]
    expected_output = {'error': 'Non unique index'}

    real_output = inner_sum(argument)

    assert_equal(real_output, expected_output)


def test_is_3():

    argument = [pd.DataFrame([1 for i in range(10)], columns = ['value'], \
        dtype= 'float64', index = [i for i in range(10)]), 
        pd.DataFrame([3 for i in range(5)], columns =  ['test'],\
        dtype= 'float64', index = [i for i in range(10, 15)])]
    expected_output = {'error': 'Element is not a timeseries: value column required'}

    real_output = inner_sum(argument)

    assert_equal(real_output, expected_output)


def test_is_4():

    argument = [pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000)])]
    expected_output = [pd.DataFrame([499500], columns = ['value'], \
        dtype = 'float64', index = [1390299700])]

    real_output = inner_sum(argument)

    test_ts_list_equality(real_output, expected_output)


def test_is_5():

    argument = [pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([1 for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390300000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([0 for i in range(10000)], columns = ['value'], \
        dtype = 'float64', index = [1390600000 + 600*i for i in range(1, 10001)])]
    expected_output = [pd.DataFrame([499500, 1000, 0], columns = ['value'], \
        dtype = 'float64', index = [1390300000, 1390600000, 1396600000])]

    real_output = inner_sum(argument)

    test_ts_list_equality(real_output, expected_output)


# --------------------------------------------------------------------
# inner_max
def test_imax_1():

    argument = [pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([1 for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390300000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([0 for i in range(10000)], columns = ['value'], \
        dtype = 'float64', index = [1390600000 + 600*i for i in range(1, 10001)])]
    expected_output = [pd.DataFrame([999, 1, 0], columns = ['value'], \
        dtype = 'float64', index = [1390300000, 1390600000, 1396600000])]

    real_output = inner_max(argument)

    test_ts_list_equality(real_output, expected_output)


def test_imax_2():

    argument = [pd.DataFrame([np.cos(i) for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1, 1001)])]
    expected_output = [pd.DataFrame([1], columns = ['value'], \
        dtype = 'float64', index = [1390300000])]

    real_output = inner_max(argument)

    test_ts_list_equality(real_output, expected_output)


def test_imax_3():

    argument = [pd.DataFrame([np.cos((np.pi * i)/(3*24)) for i in range(24)], columns = ['value'], 
        dtype = 'float64', index = [1390000000 + 3600*i for i in range(1, 25)]), \
        pd.DataFrame([np.cos((np.pi * (i + 24))/(3*24)) for i in range(24)], columns = ['value'], 
        dtype = 'float64', index = [1390086400 + 3600*i for i in range(1, 25)]), \
        pd.DataFrame([np.cos((np.pi * (i + 48))/(3*24)) for i in range(24)], columns = ['value'], 
        dtype = 'float64', index = [1390172800 + 3600*i for i in range(1, 25)])]
    expected_output = [pd.DataFrame([1, np.cos((np.pi*24)/72), np.cos((np.pi*48)/72)], columns = ['value'], \
        dtype = 'float64', index = [1390086400, 1390172800, 1390259200])]

    real_output = inner_max(argument)

    test_ts_list_equality(real_output, expected_output)

# --------------------------------------------------------------------
# inner_min
def test_imin_1():

    argument = [pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([1 for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390300000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([0 for i in range(10000)], columns = ['value'], \
        dtype = 'float64', index = [1390600000 + 600*i for i in range(1, 10001)])]
    expected_output = [pd.DataFrame([0, 1, 0], columns = ['value'], \
        dtype = 'float64', index = [1390300000, 1390600000, 1396600000])]

    real_output = inner_min(argument)

    test_ts_list_equality(real_output, expected_output)


def test_imin_2():

    argument = [pd.DataFrame([np.cos(np.pi*i/1000) for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1, 1001)])]
    expected_output = [pd.DataFrame([-1], columns = ['value'], \
        dtype = 'float64', index = [1390300000])]

    real_output = inner_min(argument)

    test_ts_list_equality(real_output, expected_output)


def test_imin_3():

    argument = [pd.DataFrame([np.cos((np.pi * i)/(3*24)) for i in range(24)], columns = ['value'], 
        dtype = 'float64', index = [1390000000 + 3600*i for i in range(1, 25)]), \
        pd.DataFrame([np.cos((np.pi * (i + 24))/(3*24)) for i in range(24)], columns = ['value'], 
        dtype = 'float64', index = [1390086400 + 3600*i for i in range(1, 25)]), \
        pd.DataFrame([np.cos((np.pi * (i + 48))/(3*24)) for i in range(24)], columns = ['value'], 
        dtype = 'float64', index = [1390172800 + 3600*i for i in range(1, 25)])]
    expected_output = [pd.DataFrame([np.cos((np.pi*23)/72), np.cos((np.pi*47)/72), 
        np.cos((np.pi*71)/72)], columns = ['value'], \
        dtype = 'float64', index = [1390086400, 1390172800, 1390259200])]

    real_output = inner_min(argument)

    test_ts_list_equality(real_output, expected_output)

# --------------------------------------------------------------------
# inner_mean

def test_imean_1():

    argument = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1, 1, 11], columns = ['value'],
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1,11)])]
    expected_output = [pd.DataFrame([2], columns = ['value'], dtype = 'float64', index = [1390003000])]

    real_output = inner_mean(argument)

    test_ts_list_equality(real_output, expected_output)


def test_imean_2():

    argument = [pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([1 for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390300000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([0 for i in range(10000)], columns = ['value'], \
        dtype = 'float64', index = [1390600000 + 600*i for i in range(1, 10001)])]
    expected_output = [pd.DataFrame([float(999*500)/1000, 1, 0], columns = ['value'], \
        dtype = 'float64', index = [1390300000, 1390600000, 1396600000])]

    real_output = inner_mean(argument)

    test_ts_list_equality(real_output, expected_output)


def test_imean_3():

    argument = [pd.DataFrame([1, 1, 'a', 1, 1, 1, 1, 1, 1, 11], columns = ['value'],
         index = [1390000000 + 300*i for i in range(1,11)])]
    expected_output =  {'error': 'Non scalar values found'}

    real_output = inner_mean(argument)

    assert_equal(real_output, expected_output)

# --------------------------------------------------------------------
# inner_std

def test_istd_1():

    argument = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1, 1, 1], columns = ['value'],
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1,11)])]
    expected_output = [pd.DataFrame([0], columns = ['value'], dtype = 'float64', index = [1390003000])]

    real_output = inner_std(argument)

    test_ts_list_equality(real_output, expected_output)


def test_istd_2():

    argument = [pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1, 1001)])]

    std = np.sqrt(np.sum([(i - 499.5)**2 for i in range(1000)])/1000)

    expected_output = [pd.DataFrame([std], columns = ['value'], \
        dtype = 'float64', index = [1390300000])]

    real_output = inner_std(argument)

    test_ts_list_equality(real_output, expected_output)


# --------------------------------------------------------------------
# last

def test_last_1():

    argument = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1, 1, 11], columns = ['value'],
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1,11)])]
    expected_output = [pd.DataFrame([11], columns = ['value'], dtype = 'float64', index = [1390003000])]

    real_output = last(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_last_2():

    argument = [pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([1 for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390300000 + 300*i for i in range(1, 1001)]), \
        pd.DataFrame([0 for i in range(10000)], columns = ['value'], \
        dtype = 'float64', index = [1390600000 + 600*i for i in range(1, 10001)])]
    expected_output = [pd.DataFrame([999, 1, 0], columns = ['value'], \
        dtype = 'float64', index = [1390300000, 1390600000, 1396600000])]

    real_output = last(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_last_3():

    argument = [pd.DataFrame([1, 1, 'a', 1, 1, 1, 1, 1, 1, 11], columns = ['value'],
         index = [1390000000 + 300*i for i in range(1,11)])]
    expected_output = [pd.DataFrame([11], columns = ['value'], dtype = 'O', index = [1390003000])]

    real_output = last(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


# --------------------------------------------------------------------
# addition
def test_add_1():

    argument = [pd.DataFrame([1 for i in range(10)], columns = ['value'], \
        dtype= 'float64', index = [i for i in range(10)]), 
        pd.DataFrame([3 for i in range(5)], columns =  ['value'],\
        dtype= 'float64', index = [i for i in range(10, 15)])]
    expected_output = {'error': 'Addition requires at least two arguments'}

    real_output = addition(argument)

    assert_equal(real_output, expected_output)


def test_add_2():

    argument = [[pd.DataFrame([1 for i in range(10)], columns = ['value'], \
        dtype= 'float64', index = [i for i in range(10)])], 
        [pd.DataFrame([3 for i in range(5)], columns =  ['value'],\
        dtype= 'float64', index = [i for i in range(1, 6)])]]
    expected_output = [pd.DataFrame([4 for i in range(5)], columns = ['value'], \
        dtype= 'float64', index = [i for i in range(1, 6)])]

    real_output = addition(*argument)

    test_ts_list_equality(real_output, expected_output)


def test_add_3():

    argument = [[pd.DataFrame(['a' for i in range(10)], columns = ['value'], \
        index = [i for i in range(10)])], 
        [pd.DataFrame([3 for i in range(5)], columns =  ['value'],\
        dtype= 'float64', index = [i for i in range(1, 6)])]]
    expected_output = {'error': 'Non scalar values found'}

    real_output = addition(*argument)

    assert_equal(real_output, expected_output)


def test_add_4():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000)]), # ts_0_0
        pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000)]), # ts_0_1
        pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000)])], # ts_0_2
        [pd.DataFrame([3 for i in range(5)], columns =  ['value'],\
        dtype= 'float64', index = [i for i in range(1, 6)])]]
    expected_output = {'error': 'Timeseries lists must have the same dimension'}

    real_output = addition(*argument)

    assert_equal(real_output, expected_output)


def test_add_5():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000)]), # ts_0_0
        pd.DataFrame([2*i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000, 2000)]), # ts_0_1
        pd.DataFrame([3*i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(2000, 3000)])], # ts_0_2
        [pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000)]), # ts_1_0
        pd.DataFrame([-i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000, 2000)]), # ts_1_1
        pd.DataFrame([-3*i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(2000, 3000)])]] # ts_1_2
    expected_output = [pd.DataFrame([2*i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000)]), # ts_0_0
        pd.DataFrame([i for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(1000, 2000)]), # ts_0_1
        pd.DataFrame([0 for i in range(1000)], columns = ['value'], \
        dtype = 'float64', index = [1390000000 + 300*i for i in range(2000, 3000)])]

    real_output = addition(*argument)

    test_ts_list_equality(real_output, expected_output)



# --------------------------------------------------------------------
# scalar_product
def test_scs_1():

    argument = [pd.DataFrame([500 for i in range(5)] + [100 for i in range(5)],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000])]
    expected_output = [pd.DataFrame([500 for i in range(5)] + [100 for i in range(5)],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500, 1393630700, 1393631000])]

    real_output = scalar_sum(argument)

    test_ts_list_equality(real_output, expected_output)


def test_scs_2():

    argument = [pd.DataFrame(['a', 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500])]
    expected_output = {'error': 'Non scalar values found'}

    real_output = scalar_sum(argument)

    assert_equal(real_output, expected_output)


def test_scs_3():

    argument = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500])]
    expected_output = {'error': 'number is not numeric'}

    real_output = scalar_sum(argument, number = 'a')

    assert_equal(real_output, expected_output)


def test_scs_4():

    argument = [pd.DataFrame([1, 1, 1, 1, 1, 1, 1, 1],\
        columns = ['value'], dtype= 'float64', \
        index = [1393628100, 1393628400, 1393628900, 1393629500, 1393629600, \
        1393629700, 1393630000, 1393630500])]
    expected_output = {'error': 'unknown argument test'}

    real_output = scalar_sum(argument, test = 1)

    assert_equal(real_output, expected_output)


def test_scs_5():

    argument = [pd.DataFrame([-1 for i in range(10)],\
        columns = ['value'], dtype= 'float64', \
        index = ['a' for i in range(10)])]
    expected_output = {'error': 'Element is not a timeserie: Integer index required'}

    real_output = scalar_sum(argument, number = -5)

    assert_equal(real_output, expected_output)


def test_scs_6():

    argument = [pd.DataFrame([-i for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)]), 
        pd.DataFrame([0 for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)])]
    expected_output = [pd.DataFrame([10 - i for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)]), 
        pd.DataFrame([10 for i in range(10)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(10)])]

    real_output = scalar_sum(argument, number = 10)

    test_ts_list_equality(real_output, expected_output)


# --------------------------------------------------------------------
# product
def test_prod_1():

    argument = [[pd.DataFrame([500 for i in range(5)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(5)])],
        [pd.DataFrame([2], columns = ['value'])]]
    expected_output = [pd.DataFrame([1000 for i in range(5)], columns = ['value'], 
        dtype= 'float64', index = [i for i in range(5)])]

    real_output = product(*argument)

    test_ts_list_equality(real_output, expected_output)


def test_prod_2():

    argument = [[pd.DataFrame([2], columns = ['value'])],
        [pd.DataFrame([2], columns = ['value'])]]
    expected_output = [pd.DataFrame([4], columns = ['value'], dtype= 'float64')]

    real_output = product(*argument)

    test_ts_list_equality(real_output, expected_output)


def test_prod_3():

    argument = [[pd.DataFrame([2], columns = ['value'])],
        [pd.DataFrame([2], columns = ['value'], index = [1])]]
    expected_output = [pd.DataFrame(columns = ['value'])]

    real_output = product(*argument)

    test_ts_list_equality(real_output, expected_output)


def test_prod_4():

    argument = [[pd.DataFrame([i for i in range(1000)])]]
    expected_output = {'error': 'Element is not a timeseries: value column required'}

    real_output = product(*argument)

    assert_equal(real_output, expected_output)


def test_prod_5():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(1000)]),
        pd.DataFrame([i for i in range(500)], columns = ['value'],
        index = [i for i in range(0, 1000, 2)])],
        [pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(0, 2000, 2)]),
        pd.DataFrame([1 for i in range(500)], columns = ['value'],
        index = [i for i in range(0, 1000, 2)]),
        pd.DataFrame([1 for i in range(500)], columns = ['value'],
        index = [i for i in range(0, 1000, 2)])]]
    expected_output = {'error': 'Product - Timeseries list must have same dimension'}

    real_output = product(*argument)

    assert_equal(real_output, expected_output)


def test_prod_6():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(1000)]),
        pd.DataFrame([i for i in range(500)], columns = ['value'],
        index = [i for i in range(0, 1000, 2)])],
        [pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(0, 2000, 2)]),
        pd.DataFrame([1 for i in range(500)], columns = ['value'],
        index = [i for i in range(0, 1000, 2)])]]
    expected_output = {'error': 'unknown argument test'}

    real_output = product(*argument, test = 'test')

    assert_equal(real_output, expected_output)


def test_prod_7():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(1000)], dtype= 'float64'),
        pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(0, 2000, 2)], dtype= 'float64')],
        [pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(0, 2000, 2)], dtype= 'float64'),
        pd.DataFrame([1 for i in range(500)], columns = ['value'],
        index = [i for i in range(500)], dtype= 'float64')]]
    expected_output = [pd.DataFrame([(2*i)*(i) for i in range(500)], columns = ['value'],
        index = [i for i in range(0, 1000, 2)], dtype= 'float64'),
        pd.DataFrame([(i)*(1) for i in range(250)], columns = ['value'],
        index = [i for i in range(0, 500, 2)], dtype= 'float64')]

    real_output = product(*argument)

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


# --------------------------------------------------------------------
# division

def test_div_1():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(1000)], dtype= 'float64'),
        pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(0, 2000, 2)], dtype= 'float64')],
        [pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(0, 2000, 2)], dtype= 'float64'),
        pd.DataFrame([1 for i in range(500)], columns = ['value'],
        index = [i for i in range(500)], dtype= 'float64')]]

    expected_output = [pd.DataFrame([2 for i in range(1, 500)], columns = ['value'],
        index = [i for i in range(2, 1000, 2)], dtype= 'float64'),
        pd.DataFrame([i for i in range(250)], columns = ['value'],
        index = [i for i in range(0, 500, 2)], dtype= 'float64')]

    real_output = division(*argument)

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_div_2():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [i for i in range(1000)], dtype= 'float64')],
        [pd.DataFrame([i % 5 for i in range(1000)], columns = ['value'],
        index = [i for i in range(1000)], dtype= 'float64')]]

    values = []
    indexs = []
    for i in range(200):
        for j in range(1, 5):
            values.append( float(5 * i + j)/ float(j) )
            indexs.append( 5 * i + j)

    expected_output = [pd.DataFrame(values, columns = ['value'],
        index = indexs, dtype= 'float64')]

    real_output = division(*argument)

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_div_3():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [1393628100 + 300*i for i in range(1000)])],
        [pd.DataFrame([1000], columns = ['value'],
        index = [1393628100], dtype= 'float64')]]

    #print argument

    expected_output = [pd.DataFrame([float(i)/1000 for i in range(1000)], columns = ['value'],
        index = [1393628100 + 300*i for i in range(1000)])]

    real_output = division(*argument)

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_div_4():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [1393628100 + 300*i for i in range(1000)])],
        [pd.DataFrame([0 for i in range(500)], columns = ['value'],
        index = [1393628100 + 600*i for i in range(500)], dtype= 'float64')]]

    #print argument

    expected_output = [pd.DataFrame(columns = ['value'])]

    real_output = division(*argument)

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)



def test_div_5():

    argument = [[pd.DataFrame([i for i in range(1000)], columns = ['value'],
        index = [1393628100 + 300*i for i in range(1000)]), 
        pd.DataFrame([i + 100 for i in range(1000)], columns = ['value'],
        index = [1393928100 + 300*i for i in range(1000)])],
        [pd.DataFrame([500], columns = ['value'], index = [1393628100], dtype= 'float64'), 
        pd.DataFrame([600], columns = ['value'], index = [1393928100], dtype= 'float64')]]

    #print argument

    expected_output = [pd.DataFrame([float(i)/500 for i in range(1000)], columns = ['value'],
        index = [1393628100 + 300*i for i in range(1000)]), 
        pd.DataFrame([(float(i) + 100)/600 for i in range(1000)], columns = ['value'],
        index = [1393928100 + 300*i for i in range(1000)])]

    real_output = division(*argument)

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


# -----------------------------------------------------------------------------
# Timeseries splitting

def test_split_1():

    argument = [pd.DataFrame([i for i in range(100)], columns = ['value'],
        index = [1393624800 + 3600*i for i in range(100)])]

    #print argument

    expected_output = [pd.DataFrame([0, 1], columns = ['value'],
        index = [1393624800, 1393628400]), # February
        pd.DataFrame([i for i in range(2, 100)], columns = ['value'],
        index = [1393624800 + 3600*i for i in range(2, 100)])]

    real_output = split(argument, period = 'month')

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_split_2():

    argument = [pd.DataFrame([0 for i in range(1401573900, 1404165900, 300)], columns = ['value'],
        index = [i for i in range(1401573900, 1404165900, 300)])]

    #print argument

    expected_output = []
    for j in range(1401573900, 1404165900, 3600):
        values = [0 for i in range(12)]
        indexs = [j + 300*i for i in range(12)]
        expected_output.append(pd.DataFrame(values, columns = ['value'], index = indexs))

    real_output = split(argument, period = 'hour')

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_split_3():

    argument = [pd.DataFrame([0 for i in range(1401573900, 1404165900, 300)], columns = ['value'],
        index = [i for i in range(1401573900, 1404165900, 300)])]

    #print argument

    expected_output = []
    for j in range(1401573900, 1404165900, 3600*24):
        values = [0 for i in range(12*24)]
        indexs = [j + 300*i for i in range(12*24)]
        expected_output.append(pd.DataFrame(values, columns = ['value'], index = indexs))

    real_output = split(argument, period = 'day')

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_split_4():

    argument = [pd.DataFrame([0 for i in range(1401573600, 1404165600, 300)], columns = ['value'],
        index = [i for i in range(1401573600, 1404165600, 300)])]

    #print argument

    expected_output = [pd.DataFrame([0 for i in range(1401573600, 1404165600, 300)], columns = ['value'],
        index = [i for i in range(1401573600, 1404165600, 300)])]

    real_output = split(argument, period = 'year')

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)



def test_split_5():

    argument = [pd.DataFrame([0 for i in range(1372629900, 1404165900, 300)], columns = ['value'],
        index = [i for i in range(1372629900, 1404165900, 300)])]

    #print argument

    l= [1372629600, #07-2013
        1375308000, # 08-2013
        1377986400, # 09-2013
        1380578400, # 10-2013
        1383260400, # 11-2013
        1385852400, # 12-2013
        1388530800, # 01-2014
        1391209200, # 02-2014
        1393628400, # 03-2014
        1396303200, # 04-2014
        1398895200, # 05-2014
        1401573600, # 06-2014
        1404165600] # 07-2014
    
    expected_output = []
    for i in range(12):
        values = [0 for j in range(l[i] + 300, l[i + 1] + 300, 300)]
        indexs = [j for j in range(l[i] + 300, l[i + 1] + 300, 300)]
        expected_output.append(pd.DataFrame(values, columns = ['value'], index = indexs))

    real_output = split(argument, period = 'month')

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


# ---------------------------------------------------------------------------------------------
# timeseries list generation
def tsl_gen_test_1():

    argument = json.dumps([{'value' : [0 for i in range(1372629600, 1404165600, 300)], 
        'index' : [i for i in range(1372629600, 1404165600, 300)]}])

    expected_output = [pd.DataFrame([0 for i in range(1372629600, 1404165600, 300)], columns = ['value'],
        index = [i for i in range(1372629600, 1404165600, 300)])]

    real_output = generate_ts_list(argument)

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


# ----------------------------------------------------------------------------------------------
# ts_list to list
def tstl_test_1():

    argument = [pd.DataFrame([0 for i in range(1372629600, 1404165600, 300)], columns = ['value'],
        index = [i for i in range(1372629600, 1404165600, 300)])]

    expected_output = [[[i, 0] for i in range(1372629600, 1404165600, 300)]]

    real_output = ts_list_to_list(argument)

    #print real_output, expected_output

    assert_equal(real_output, expected_output)


def tstl_test_2():

    l= [1372629600, #07-2013
        1375308000, # 08-2013
        1377986400, # 09-2013
        1380578400, # 10-2013
        1383260400, # 11-2013
        1385852400, # 12-2013
        1388530800, # 01-2014
        1391209200, # 02-2014
        1393628400, # 03-2014
        1396303200, # 04-2014
        1398895200, # 05-2014
        1401573600, # 06-2014
        1404165600] # 07-2014
    
    argument = []
    for i in range(12):
        values = [0 for j in range(l[i] + 300, l[i + 1] + 300, 300)]
        indexs = [j for j in range(l[i] + 300, l[i + 1] + 300, 300)]
        argument.append(pd.DataFrame(values, columns = ['value'], index = indexs))

    expected_output = []
    for i in range(12):
        values = [0 for j in range(l[i] + 300, l[i + 1] + 300, 300)]
        indexs = [j for j in range(l[i] + 300, l[i + 1] + 300, 300)]
        expected_output.append([[indexs[i], values[i]] for i in range(len(values))])

    real_output = ts_list_to_list(argument)

    #print real_output, expected_output

    assert_equal(real_output, expected_output)


