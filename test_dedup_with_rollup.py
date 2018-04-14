from unittest import TestCase
import pandas as pd

from resample_example import dedup_with_rollup


class TestDedup_with_rollup(TestCase):

    def test_rollup_df_basic(self):
        input_df = pd.DataFrame(
            [['a', 'a', 'some data',    None],
             ['a', 'a', None,           'some data 2'],
             ['b', 'b', 'other data',   'other data 2'],
             ['b', 'b', None,           None]
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
