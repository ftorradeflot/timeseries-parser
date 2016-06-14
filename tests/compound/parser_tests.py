# --------------------------------------------------------------------
# Author: Francesc Torradeflot - <ciscu@nomorecode.com>
#
# Description:
# Tests on analysis/analysis_parser.py
#
# --------------------------------------------------------------------
# Copyright (c) 2014 - All Rights Reserved.
#
# This source is subject to the Nomorecode Source License.
# Please see the License.md file for more information, which is
# part of this source code package.
# --------------------------------------------------------------------

# --------------------------------------------------------------------
# --------------------------------------------------------------------
# Imports and defines.
from nose.tools import *
import sys
sys.path.append('../../src')
sys.path.append('analysis')
print sys.path
import json

from compound.parser import *
from timeseries_functions_tests import test_ts_list_equality
import pandas as pd

# ------------------------------- is_kwarg --------------------------------

def test_ik_1():

    argument = 'test('

    expected_output = ('arg', 'test(', None)

    real_output = is_kwarg(argument)

    assert_equal(real_output, expected_output)


def test_ik_2():

    argument = 'test=='

    expected_output = ('kwarg', 'test', '=')

    real_output = is_kwarg(argument)

    assert_equal(real_output, expected_output)


def test_ik_3():

    argument = 'test()'

    expected_output = ('arg', 'test()', None)

    real_output = is_kwarg(argument)

    assert_equal(real_output, expected_output)


def test_ik_4():

    argument = '=te=/()'

    expected_output = ('error', 'Invalid syntax', None)

    real_output = is_kwarg(argument)

    assert_equal(real_output, expected_output)


# ---------------------------- parse_args ----------------------------------

def test_pa_1():
    
    argument = 'lsfsaldjf;alkfjasld,dsj;()'

    expected_output = (['lsfsaldjf', 'alkfjasld,dsj', '()'], {})

    real_output = parse_args(argument)

    assert_equal(real_output, expected_output)


def test_pa_2():
    
    argument = 'lsfs=aldjf;alkfja=sld,dsj;()'

    expected_output = (['()'], {'lsfs':'aldjf', 'alkfja':'sld,dsj'})

    real_output = parse_args(argument)

    assert_equal(real_output, expected_output)


def test_pa_3():
    
    argument = '==aldjf;alkfja=sld,dsj;()'

    expected_output = ('error', 'Invalid syntax')

    real_output = parse_args(argument)

    assert_equal(real_output, expected_output)


# ---------------------------- find_funcs ----------------------------------


def test_ff_1():
    
    argument = '==aldjf;alkfja=sld,dsj;()'

    expected_output = ('success', '==aldjf;alkfja=sld,dsj;', '')

    real_output = find_func(argument)

    assert_equal(real_output, expected_output)


def test_ff_2():
    
    argument = 'alfjsdlj((),falsdjf))'

    expected_output = ('success', 'alfjsdlj', '(),falsdjf)')

    real_output = find_func(argument)

    assert_equal(real_output, expected_output)


def test_ff_3():
    
    argument = '(alfjsdlj((),falsdjf))'

    expected_output = ('success', '', 'alfjsdlj((),falsdjf)')

    real_output = find_func(argument)

    print real_output, expected_output

    assert_equal(real_output, expected_output)


# ------------------------------------- Tests on analysis_parser ---------------------------

def test_ap_1():
    
    argument = 'generate_ts_list([{"value":[0], "index":[0]}])'

    expected_output = [pd.DataFrame([0], columns = ['value'], index = [0])]

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_2():

    ts_list_text = '[{"value":[0, 1, 1], "index":[1393628100, 1393628400, 1393628900]}]'
    argument = 'distribute_ts_list(generate_ts_list(' + ts_list_text + '); e_from = 1393627000)'

    expected_output = [pd.DataFrame([0, 0, 0, 0, 1, 1, 1], columns = ['value'], \
    index = [1393627200 + 300* i for i in range(7)], dtype = 'float64')]

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


INDEX_LIST = [1396631712, 1396783500, 1396938900, 1397091000, 
        1397246400, 1397398200, 1397549700, 1397701500, 1397853003, 1398004800, 
        1398156900, 1398308919, 1398470716, 1398657600, 1398809646, 1398961500, 
        1399114500, 1399266600, 1399436982, 1399588854, 1399740726, 1399893600, 
        1400045673, 1400234536, 1400541889, 1400847437, 1401154790, 1401516846]
INDEX_LIST_ST = json.dumps(INDEX_LIST)

VALUE_LIST = [u'2.6', u'2.6', u'2.6', u'2.6', u'201.42', u'711.39', \
        u'1032.38', u'1708.79', u'2312.31', u'2988.04', u'3262.0', u'3866.57', u'4384.74', \
        u'4976.11', u'5706.74', u'6071.85', u'6591.16', u'7129.17', u'7852.06', u'8346.7', \
        u'8933.81', u'9455.72', u'10138.71', u'10790.05', u'11714.37', u'13010.38', u'14127.52', \
        u'15513.87']
VALUE_LIST_ST = json.dumps(VALUE_LIST)

def test_ap_3():

    ts_list_text = '[{"value":' + VALUE_LIST_ST + ', "index":' + INDEX_LIST_ST + '}]'

    argument = 'generate_ts_list(' + ts_list_text + ')'

    expected_output = [pd.DataFrame(VALUE_LIST, columns = ['value'],
        index = INDEX_LIST)]
    
    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_4():

    ts_list_text = '[{"value":' + VALUE_LIST_ST + ', "index":' + INDEX_LIST_ST + '}]'

    argument = 'distribute_ts_list(generate_ts_list(' + ts_list_text + ');' +\
        ' seconds=3600; e_from = 1398895201; e_to= 1401573600)'

    output_list = []
    output_list += [u'5706.74' for i in range(1398898800, 1398960001, 3600)]
    output_list += [u'6071.85' for i in range(1398963600, 1399111201, 3600)]
    output_list += [u'6591.16' for i in range(1399114800, 1399266001, 3600)]
    output_list += [u'7129.17' for i in range(1399269600, 1399435201, 3600)]
    output_list += [u'7852.06' for i in range(1399438800, 1399586401, 3600)]
    output_list += [u'8346.7'  for i in range(1399590000, 1399737601, 3600)]
    output_list += [u'8933.81' for i in range(1399741200, 1399892401, 3600)]
    output_list += [u'9455.72' for i in range(1399896000, 1400043601, 3600)]
    output_list += [u'10138.71' for i in range(1400047200, 1400234401, 3600)]
    output_list += [u'10790.05' for i in range(1400238000, 1400540401, 3600)]
    output_list += [u'11714.37' for i in range(1400544000, 1400846401, 3600)]
    output_list += [u'13010.38' for i in range(1400850000, 1401152401, 3600)]
    output_list += [u'14127.52' for i in range(1401156000, 1401516001, 3600)]
    output_list += [u'15513.87' for i in range(1401519600, 1401573601, 3600)]

    expected_output = [pd.DataFrame(output_list, columns = ['value'],
        index = [1398895200 + i*3600 for i in range(1, 745)])]
    
    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_5():

    ts_list_text = '[{"value":' + VALUE_LIST_ST + ', "index":' + INDEX_LIST_ST + '}]'

    argument = 'increments(distribute_ts_list(generate_ts_list(' + ts_list_text + ');' +\
        ' seconds=3600; e_from = 1398895201; e_to= 1401573600))'

    output_list = []
    output_list += [0 for i in range(1398902400, 1398960001, 3600)]
    output_list += [365.11]
    output_list += [0 for i in range(1398967200, 1399111201, 3600)]
    output_list += [519.31]
    output_list += [0 for i in range(1399118400, 1399266001, 3600)]
    output_list += [538.01]
    output_list += [0 for i in range(1399273200, 1399435201, 3600)]
    output_list += [722.89]
    output_list += [0 for i in range(1399442400, 1399586401, 3600)]
    output_list += [494.64]
    output_list += [0  for i in range(1399593600, 1399737601, 3600)]
    output_list += [587.11]
    output_list += [0 for i in range(1399744800, 1399892401, 3600)]
    output_list += [521.91]
    output_list += [0 for i in range(1399899600, 1400043601, 3600)]
    output_list += [682.99]
    output_list += [0 for i in range(1400050800, 1400234401, 3600)]
    output_list += [651.34]
    output_list += [0 for i in range(1400241600, 1400540401, 3600)]
    output_list += [924.32]
    output_list += [0 for i in range(1400547600, 1400846401, 3600)]
    output_list += [1296.01]
    output_list += [0 for i in range(1400853600, 1401152401, 3600)]
    output_list += [1117.14]
    output_list += [0 for i in range(1401159600, 1401516001, 3600)]
    output_list += [1386.35]
    output_list += [0 for i in range(1401523200, 1401573601, 3600)]

    expected_output = [pd.DataFrame(output_list, columns = ['value'],
        index = [1398898800 + i*3600 for i in range(1, 744)])]
    
    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_6():

    ts_list_text = '[{"value":' + VALUE_LIST_ST + ', "index":' + INDEX_LIST_ST + '}]'

    argument = 'split(increments(distribute_ts_list(generate_ts_list(' + ts_list_text + ');' +\
        ' seconds=3600; e_from = 1398895201; e_to= 1401573600)))'

    output = []

    # 1
    output_list = [0 for i in range(1398902400, 1398960001, 3600)]
    output_list += [365.11]
    output_list += [0 for i in range(1398967200, 1398981601, 3600)]
    output.append(output_list)

    # 2
    output_list = [0 for i in range(1398985200, 1399068001, 3600)]
    output.append(output_list)

    # 3
    output_list = [0 for i in range(1399071600, 1399111201, 3600)]
    output_list += [519.31]
    output_list += [0 for i in range(1399118400, 1399154401, 3600)]
    output.append(output_list)

    # 4
    output_list = [0 for i in range(1399158000, 1399240801, 3600)]
    output.append(output_list)

    # 5
    output_list = [0 for i in range(1399244400, 1399266001, 3600)]
    output_list += [538.01]
    output_list += [0 for i in range(1399273200, 1399327201, 3600)]
    output.append(output_list)

    # 6
    output_list = [0 for i in range(1399330800, 1399413601, 3600)]
    output.append(output_list)

    # 7
    output_list = [0 for i in range(1399417200, 1399435201, 3600)]
    output_list += [722.89]
    output_list += [0 for i in range(1399442400, 1399500001, 3600)]
    output.append(output_list)

    # 8
    output_list = [0 for i in range(1399503600, 1399586401, 3600)]
    output.append(output_list)

    # 9
    output_list = [494.64]
    output_list += [0  for i in range(1399593600, 1399672801, 3600)]
    output.append(output_list)

    # 10
    output_list = [0  for i in range(1399676400, 1399737601, 3600)]
    output_list += [587.11]
    output_list += [0 for i in range(1399744800, 1399759201, 3600)]
    output.append(output_list)

    # 11
    output_list = [0 for i in range(1399762800, 1399845601, 3600)]
    output.append(output_list)

    # 12
    output_list = [0 for i in range(1399849200, 1399892401, 3600)]
    output_list += [521.91]
    output_list += [0 for i in range(1399899600, 1399932001, 3600)]
    output.append(output_list)

    # 13
    output_list = [0 for i in range(1399935600, 1400018401, 3600)]
    output.append(output_list)

    # 14
    output_list = [0 for i in range(1400022000,1400043601, 3600)]
    output_list += [682.99]
    output_list += [0 for i in range(1400050800, 1400104801, 3600)]
    output.append(output_list)

    # 15
    output_list = [0 for i in range(1400108400, 1400191201, 3600)]
    output.append(output_list)

    # 16
    output_list = [0 for i in range(1400194800,1400234401, 3600)]
    output_list += [651.34]
    output_list += [0 for i in range(1400241600, 1400277601, 3600)]
    output.append(output_list) 

    # 17
    output_list = [0 for i in range(1400281200, 1400364001, 3600)]
    output.append(output_list) 

    # 18
    output_list = [0 for i in range(1400367600, 1400450401, 3600)]
    output.append(output_list) 

    # 19
    output_list = [0 for i in range(1400454000, 1400536801, 3600)]
    output.append(output_list) 

    # 20
    output_list = [0 for i in range(1400540400, 1400540401, 3600)]
    output_list += [924.32]
    output_list += [0 for i in range(1400547600, 1400623201, 3600)]
    output.append(output_list) 

    # 21
    output_list = [0 for i in range(1400626800, 1400709601, 3600)]
    output.append(output_list) 

    # 22
    output_list = [0 for i in range(1400713200, 1400796001, 3600)]
    output.append(output_list) 

    # 23
    output_list = [0 for i in range(1400799600, 1400846401, 3600)]
    output_list += [1296.01]
    output_list += [0 for i in range(1400853600, 1400882401, 3600)]
    output.append(output_list) 

    # 24
    output_list = [0 for i in range(1400886000, 1400968801, 3600)]
    output.append(output_list) 

    # 25
    output_list = [0 for i in range(1400971400, 1401055201, 3600)]
    output.append(output_list) 

    # 26
    output_list = [0 for i in range(1401058800, 1401141601, 3600)]
    output.append(output_list) 

    # 27
    output_list = [0 for i in range(1401145200, 1401152401, 3600)]
    output_list += [1117.14]
    output_list += [0 for i in range(1401159600, 1401228001, 3600)]
    output.append(output_list) 

    # 28
    output_list = [0 for i in range(1401231600, 1401314401, 3600)]
    output.append(output_list) 

    # 29
    output_list = [0 for i in range(1401318000, 1401400801, 3600)]
    output.append(output_list) 

    # 30
    output_list = [0 for i in range(1401404400, 1401487201, 3600)]
    output.append(output_list) 

    # 31
    output_list = [0 for i in range(1401490800, 1401516001, 3600)]
    output_list += [1386.35]
    output_list += [0 for i in range(1401523200, 1401573601, 3600)]
    output.append(output_list)

    index = []
    j = 1398902400
    for o in output:
        index_list = []
        for l in o:
            index_list.append(j)
            j += 3600
        index.append(index_list)

    expected_output = []
    for i, o in enumerate(output):
        expected_output.append(pd.DataFrame(o, columns = ['value'],
        index = index[i], dtype = 'float64'))
    
    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)



def test_ap_7():

    ts_list_text = '[{"value":' + VALUE_LIST_ST + ', "index":' + INDEX_LIST_ST + '}]'

    argument = 'inner_sum(split(increments(distribute_ts_list(generate_ts_list(' + ts_list_text + ');' +\
        ' seconds=3600; e_from = 1398895201; e_to= 1401573600))))'


    output_list = []
    output_list += [365.11] #1
    output_list += [0]
    output_list += [519.31]
    output_list += [0]
    output_list += [538.01] #5
    output_list += [0]
    output_list += [722.89]
    output_list += [0]
    output_list += [494.64]
    output_list += [587.11] #10
    output_list += [0]
    output_list += [521.91]
    output_list += [0]
    output_list += [682.99]
    output_list += [0] #15
    output_list += [651.34]
    output_list += [0]
    output_list += [0]
    output_list += [0]
    output_list += [924.32] #20
    output_list += [0]
    output_list += [0]
    output_list += [1296.01]
    output_list += [0]
    output_list += [0] #25
    output_list += [0]
    output_list += [1117.14]
    output_list += [0]
    output_list += [0]
    output_list += [0] #30
    output_list += [1386.35]

    index_list = [1398981600, 1399068000, 1399154400, 1399240800, 1399327200,
         1399413600, 1399500000, 1399586400, 1399672800, 1399759200, 1399845600,
         1399932000, 1400018400, 1400104800, 1400191200, 1400277600, 1400364000,
         1400450400, 1400536800, 1400623200, 1400709600, 1400796000, 1400882400,
         1400968800, 1401055200, 1401141600, 1401228000, 1401314400, 1401400800,
         1401487200, 1401573600]

    expected_output = [pd.DataFrame(output_list, columns = ['value'],
        index = index_list, dtype = 'float64')]
    
    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_8():

    argument = 'inner_sum(increments(get_variable(2419; from = 1398895200; to = 1401573600)))'

    expected_output = [pd.DataFrame([9948.], columns = ['value'],
        index = [1401573600], dtype = 'float64')]    

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_9():

    argument = 'inner_sum(split(increments(get_variable(2419; from = 1398895200;' +\
        'to = 1401573600)); period = week))'

    expected_output = [pd.DataFrame([1303.30, 2186.09, 2076.1, 2359.8, 2022.7], columns = ['value'],
        index = [1399240800, 1399845600, 1400450400, 1401055200, 1401573600], dtype = 'float64')]   

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_10():

    argument = 'inner_sum(increments(get_variable(2421; from = 1398895200; to = 1401573600)))'

    expected_output = [pd.DataFrame([8412.07], columns = ['value'],
        index = [1401573600], dtype = 'float64')]    

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_11():

    argument = 'inner_sum(split(increments(get_variable(2421; from = 1398895200;' +\
        'to = 1401573600)); period = week))'

    expected_output = [pd.DataFrame([943.35, 1828.29, 2099.66, 1847.26, 1693.51], columns = ['value'],
        index = [1399240800, 1399845600, 1400450400, 1401055200, 1401573600], dtype = 'float64')]   

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_12():

    argument = 'addition(inner_sum(split(increments(get_variable(2419; from = 1398895200;' +\
        'to = 1401573600)); period = week)); inner_sum(split(increments(get_variable(2421;' +\
        'from = 1398895200; to = 1401573600)); period = week)))'

    expected_output = [pd.DataFrame([2246.66, 4014.38, 4175.76, 4207.06, 3716.2], columns = ['value'],
        index = [1399240800, 1399845600, 1400450400, 1401055200, 1401573600], dtype = 'float64')]   

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_13():

    incs_1 = 'increments(get_variable(2419; from = 1398895200; to = 1401573600))'
    incs_2 = 'increments(get_variable(2421; from = 1398895200; to = 1401573600))'

    argument = 'inner_sum(split(addition(' + incs_1 + ';' + incs_2 +'); period = week))'

    expected_output = [pd.DataFrame([2246.66, 4014.38, 4175.76, 4207.06, 3716.2], columns = ['value'],
        index = [1399240800, 1399845600, 1400450400, 1401055200, 1401573600], dtype = 'float64')]   

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_14():

    incs_1 = 'increments(get_variable(2419; from = 1398895200; to = 1401573600))'
    incs_2 = 'increments(get_variable(2421; from = 1398895200; to = 1401573600))'

    argument = 'scalar_product(inner_sum(split(addition(' + incs_1 + \
        ';' + incs_2 +'); period = week)); number = 0.5)'

    expected_output = [pd.DataFrame([1123.33, 2007.19, 2087.88, 2103.53, 1858.1], columns = ['value'],
        index = [1399240800, 1399845600, 1400450400, 1401055200, 1401573600], dtype = 'float64')]   

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_15():

    # compute increments between epoch 1399200000 and 1399250000
    # when the oven is off

    # Get the increments of the gas meter
    incs = 'increments(get_variable(2419; from = 1399200000; to = 1399250000))'

    # Get on/off (1/0) value and change it to 0/1
    state = 'scalar_sum(scalar_product(get_variable(2420; from = 1399200000;' +\
        'to = 1399250000); number = -1); number = 1)'

    # Compute the product and sum
    argument = 'inner_sum(product(' + incs + ';' + state + '))'

    expected_output = [pd.DataFrame([13.5], columns = ['value'],
        index = [1399250100], dtype = 'float64')]   

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_16():

    # compare hourly incrementes splitting the data before and 
    # after calculating the increments

    # If we perform the subtraction before the sum, non matching epochs
    # are discarded when doing the subtraction and the result is equal to 0

    # split before computing the increments
    incs_1 = 'increments(split(get_variable(2419; from = 1398895201; to = 1401573600); period=hour))'
    
    # split after computing the increments
    incs_2 = 'split(increments(get_variable(2419; from = 1398895201; to = 1401573600)); period=hour)'

    argument = 'inner_sum(inner_sum(subtraction(' + incs_1 + ';' + incs_2 + ')))'

    expected_output = [pd.DataFrame([0.], columns = ['value'],
        index = [1401573600], dtype = 'float64')]   

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_17():

    # compare hourly incrementes splitting the data before and 
    # after calculating the increments

    # If we split before computing the increments, one value every hour will be
    # lost and the totals will be different

    # split before computing the increments
    incs_1 = 'inner_sum(inner_sum(increments(split(get_variable(2419; ' +\
        'from = 1398895201; to = 1401573600); period=hour))))'
    
    # split after computing the increments
    incs_2 = 'inner_sum(inner_sum(split(increments(get_variable(2419; ' +\
        'from = 1398895201; to = 1401573600)); period=hour)))'

    argument = 'subtraction(' + incs_1 + ';' + incs_2 + ')'

    expected_output = [pd.DataFrame([-802.03], columns = ['value'],
        index = [1401573600], dtype = 'float64')]   

    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_18():

    # check that, when using closed intervals
    # we obtain the total increments of the period

    argument = 'inner_sum(increments(get_variable(2419; ' +\
        'now = 1401524000; int_type = closed; range=this_hour)))'   

    expected_output = [pd.DataFrame([30.6], columns = ['value'],
        index = [1401526800], dtype = 'float64')] 
 
    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)


def test_ap_19():

    # increments per week
    arg_1 = 'inner_sum(split(increments(get_variable(2419; ' +\
        'from = 1398895201; to = 1401573600)); period=week))'

    # weekly price
    arg_2 = 'generate_ts_list([{"value":[1, 10, 100, 1000, 10000], ' +\
        '"index":[1399240800, 1399845600, 1400450400, 1401055200, 1401573600]}])'

    argument = 'product(' + arg_1 + ';' + arg_2 + ')'

    expected_output = [pd.DataFrame([1303.3, 21860.9, 207610., 2359800., 20227000.], columns = ['value'],
        index = [1399240800, 1399845600, 1400450400, 1401055200, 1401573600], dtype = 'float64')] 
 
    real_output = parser(argument)

    print real_output, expected_output

    test_ts_list_equality(real_output, expected_output)

#def test_ap_20():


