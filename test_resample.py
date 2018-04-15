from unittest import TestCase
import pandas as pd
from datetime import datetime

from resample_example import dedup_with_rollup, make_time_seres


def mk_date(str_date):
    return datetime.strptime(str_date, '%m/%d/%Y')


t1 = 'type1'
t2 = 'type2'


class TestResample(TestCase):

    def test_resample(self):
        input_df = pd.DataFrame(
            [
                [mk_date('04/12/2018'), t1],
                [mk_date('04/13/2018'), t2],
                [mk_date('04/16/2018'), t1],
                [mk_date('04/16/2018'), t2],
                [mk_date('04/16/2018'), t2],
                [mk_date('04/16/2018'), t2],
            ],
            columns=['EventDatetime', 'EventType']
        )
        expected_df = pd.DataFrame(
            [
                [1.0, 0.0],
                [0.0, 1.0],
                [0.0, 0.0],
                [0.0, 0.0],
                [1.0, 3.0],
            ],
            columns=['type1', 'type2'],
            index=[mk_date('04/12/2018'), mk_date('04/13/2018'), mk_date('04/14/2018'), mk_date('04/15/2018'), mk_date('04/16/2018')]
        )
        actual_df = make_time_seres(input_df)
        self.assertTrue(expected_df.equals(actual_df))

    def test_rollup_df_basic(self):
        input_df = pd.DataFrame(
            [['a', 'a', 'some data', None],
             ['a', 'a', None, 'some data 2'],
             ['b', 'b', 'other data', 'other data 2'],
             ['b', 'b', None, None]
             ],
            columns=['uniq1', 'uniq2', 'Data1', 'Data2']
        )
        expected_df = pd.DataFrame(
            [['a', 'a', 'some data', 'some data 2'],
             ['b', 'b', 'other data', 'other data 2']
             ],
            columns=['uniq1', 'uniq2', 'Data1', 'Data2']
        )
        actual_df = dedup_with_rollup(input_df, ['uniq1', 'uniq2'])
        self.assertTrue(expected_df.equals(actual_df))

    def test_rollup_df_take_last(self):
        input_df = pd.DataFrame(
            [['a', 'a', 'some data'],
             ['a', 'a', None],
             ['b', 'b', 'other data'],
             ['a', 'a', 'newer data'],
             ],
            columns=['uniq1', 'uniq2', 'Data']
        )
        expected_df = pd.DataFrame(
            [['a', 'a', 'newer data'],
             ['b', 'b', 'other data']
             ],
            columns=['uniq1', 'uniq2', 'Data']
        )
        actual_df = dedup_with_rollup(input_df, ['uniq1', 'uniq2'])
        self.assertTrue(expected_df.equals(actual_df))

    def test_rollup_single_unique_col(self):
        input_df = pd.DataFrame(
            [['a', 'some data'],
             ['a', None],
             ['b', 'other data'],
             ['a', 'newer data'],
             ],
            columns=['uniq1', 'Data']
        )
        expected_df = pd.DataFrame(
            [['a', 'newer data'],
             ['b', 'other data']
             ],
            columns=['uniq1', 'Data']
        )
        actual_df = dedup_with_rollup(input_df, ['uniq1'])
        self.assertTrue(expected_df.equals(actual_df))
