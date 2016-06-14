# --------------------------------------------------------------------
# Author: Francesc Torradeflot - <ciscu@nomorecode.com>
#
# Description:
# Tests on the usage function from analysis/timeseries_utils.py
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
from timeseries_functions_tests import test_ts_list_equality

def usage_test_1():

    # This is the result from: 
    # inner_sum(increments(get_variable(2419, range='this_week', 
    # now =1400104800, int_type='closed')))
    expected_output = [pd.DataFrame([2076.1],
        columns = ['value'], dtype = 'float64',
        index = [1400450400])]

    real_output = usage(2419, range = 'this_week', now = 1400104800)

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def usage_test_2():

    # This is the result from: 
    
    expected_output = increments(get_variable(2419, count = 2, now = 1400104800))

    real_output = usage(2419, group_by = 'instant', now = 1400104800)

    #print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)
