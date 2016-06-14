# --------------------------------------------------------------------
# Author: Francesc Torradeflot - <ciscu@nomorecode.com>
#
# Description:
# Tests on analysis_functions.py
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

from analysis.analysis_functions import *

# Fake objects created for testing

TS_1 = [[(1356994800, 1), #1/1/2013 0:0:0
(1388530800, 0),  #1/1/2014 0:0:0
(1391209200, 1), #1/2/2014 0:0:0
(1391295600, 0), #2/2/2014 0:0:0
(1391986800, 1), #10/2/2013 0:0:0
(1392073200, 0),  #11/2/2014 0:0:0
(1393282800, 1), #25/2/2014 0:0:0
(1393628400, 0)]] #1/3/2014 0:0:0

TS_2 = [[(1356994800, 1), #1/1/2013 0:0:0
(1388530800, 0),  #1/1/2014 0:0:0
(1391209200, 1), #1/2/2014 0:0:0
(1391295600, 0)], #2/2/2014 0:0:0
[(1391986800, 1), #10/2/2013 0:0:0
(1392073200, 0),  #11/2/2014 0:0:0
(1393282800, 1), #25/2/2014 0:0:0
(1393628400, 0)]] #1/3/2014 0:0:0 is considered as part of february


# --------------------------------------------------------------------
# time_series_group_by

# Test time_series_group_by, grouping by month
def test_tsgb_month():
    result = [[(1356994800, 1)], [(1359673200, None)], [(1362092400, None)], [(1364767200, None)], 
        [(1367359200, None)], [(1370037600, None)], [(1372629600, None)], [(1375308000, None)], 
        [(1377986400, None)], [(1380578400, None)], [(1383260400, None)], [(1388530800, 0)], 
        [(1388530800, 0), (1391209200, 1)], [(1391209200, 1), (1391295600, 0), (1391986800, 1), 
        (1392073200, 0), (1393282800, 1), (1393628400, 0)]]

    ts_2 = timeseries_group_by(TS_1, 'month')

    print ts_2, result

    assert_equal(ts_2, result)

# Test time_series_group_by, grouping by year
def test_tsgb_year():
    result = [[(1356994800, 1), (1388530800, 0)], [(1388530800, 0), (1391209200, 1),
        (1391295600, 0), (1391986800, 1), (1392073200, 0), (1393282800, 1), (1393628400, 0)]]

    ts_2 = timeseries_group_by(TS_1, 'year')
    assert_equal(ts_2, result)


# --------------------------------------------------------------------
# count_state_change

def test_1_csc_1():
    assert_equal(count_state_change(TS_1, 1), [[(1356994800, 3)]])

def test_1_csc_0():
    assert_equal(count_state_change(TS_1, 0), [[(1356994800, 4)]])

def test_1_csc_a():
    assert_equal(count_state_change(TS_1, 'a'), [[(1356994800, 0)]])

def test_2_csc_1():
    assert_equal(count_state_change(TS_2, 1), [[(1356994800, 1), (1391986800, 1)]])

def test_2_csc_0():
    assert_equal(count_state_change(TS_2, 0), [[(1356994800, 2), (1391986800, 2)]])

def test_2_csc_a():
    assert_equal(count_state_change(TS_2, 'a'), [[(1356994800, 0), (1391986800, 0)]])


# --------------------------------------------------------------------
# timeseries_to_float

# Objects specificly created for testing timeseries_to_float
TS_3 = [[(1356994800, '1')]]
TS_4 = [[(1356994800, 1)]]
TS_5 = [[(1356994800, '1.3')]]
TS_6 = [[(1356994800, 'a')]]

def test_ttf_1():
    assert_equal(timeseries_to_float(TS_3), [[(1356994800, 1.)]])

def test_ttf_2():
    assert_equal(timeseries_to_float(TS_4), [[(1356994800, 1.)]])

def test_ttf_3():
    assert_equal(timeseries_to_float(TS_5), [[(1356994800, 1.3)]])

def test_ttf_4():
    assert_equal(timeseries_to_float(TS_6), {'error': json.dumps({'error': 'Invalid value a received'})})


# --------------------------------------------------------------------
# compute_meter_increments

TS_7 = [[(1356994800, 1), #1/1/2013 0:0:0
(1388530800, 0),  #1/1/2014 0:0:0
(1391209200, 10), #1/2/2014 0:0:0
(1391295600, 0), #2/2/2014 0:0:0
(1391986800, 20), #10/2/2013 0:0:0
(1392073200, 0),  #11/2/2014 0:0:0
(1393282800, 30), #25/2/2014 0:0:0
(1393628400, 0)]] #1/3/2014 0:0:0

def test_cmi_1():
    assert_equal(compute_meter_increments(TS_7), [[(1356994800, 60)]])

TS_8 = [[]]

def test_cmi_2():
    assert_equal(compute_meter_increments(TS_8), {'error': json.dumps({'error': 'Invalid timeserie received'})})

TS_9 = [[(1356994800, 1), #1/1/2013 0:0:0
(1388530800, 0),  #1/1/2014 0:0:0
(1391209200, 10), #1/2/2014 0:0:0
(1391295600, 0)], #2/2/2014 0:0:0
[(1391986800, 20), #10/2/2013 0:0:0
(1392073200, 0),  #11/2/2014 0:0:0
(1393282800, 30), #25/2/2014 0:0:0
(1393628400, 0)]] #1/3/2014 0:0:0

def test_cmi_3():
    assert_equal(compute_meter_increments(TS_9), [[(1356994800, 10), (1391986800, 30)]])

# --------------------------------------------------------------------
# scalar_product

def test_sp_1():
    assert_equal(scalar_product(TS_9, 'a'), {'error': json.dumps({'error': 'Value received is not a number: a'})})

def test_sp_2():
    expected_result = [[(1356994800, 0), #1/1/2013 0:0:0
    (1388530800, 0),  #1/1/2014 0:0:0
    (1391209200, 0), #1/2/2014 0:0:0
    (1391295600, 0)], #2/2/2014 0:0:0
    [(1391986800, 0), #10/2/2013 0:0:0
    (1392073200, 0),  #11/2/2014 0:0:0
    (1393282800, 0), #25/2/2014 0:0:0
    (1393628400, 0)]] #1/3/2014 0:0:0

    assert_equal(scalar_product(TS_9, 0), expected_result)

def test_sp_3():
    expected_result = [[(1356994800, 0.1), #1/1/2013 0:0:0
    (1388530800, 0),  #1/1/2014 0:0:0
    (1391209200, 1), #1/2/2014 0:0:0
    (1391295600, 0)], #2/2/2014 0:0:0
    [(1391986800, 2), #10/2/2013 0:0:0
    (1392073200, 0),  #11/2/2014 0:0:0
    (1393282800, 3), #25/2/2014 0:0:0
    (1393628400, 0)]] #1/3/2014 0:0:0

    assert_equal(scalar_product(TS_9, 0.1), expected_result)

def test_sp_4():
    expected_result = [[(1356994800, 1000), #1/1/2013 0:0:0
    (1388530800, 0),  #1/1/2014 0:0:0
    (1391209200, 10000), #1/2/2014 0:0:0
    (1391295600, 0)], #2/2/2014 0:0:0
    [(1391986800, 20000), #10/2/2013 0:0:0
    (1392073200, 0),  #11/2/2014 0:0:0
    (1393282800, 30000), #25/2/2014 0:0:0
    (1393628400, 0)]] #1/3/2014 0:0:0

    assert_equal(scalar_product(TS_9, 1000), expected_result)

