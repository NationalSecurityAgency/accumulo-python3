import unittest

from accumulo.core import structs


class TestStructs(unittest.TestCase):

    def test_authorization_set(self):
        auth_set_1 = structs.AuthorizationSet({'A', 'B'})
        auth_set_1_binary = structs.AuthorizationSet({b'A', b'B'})
        self.assertEqual(auth_set_1, auth_set_1_binary)
        self.assertEqual(auth_set_1, frozenset({b'A', b'B'}))
        self.assertIn('A', auth_set_1)
        auth_set_2 = structs.AuthorizationSet({'B', 'C'})
        self.assertEqual(auth_set_1 | auth_set_2, frozenset({b'A', b'B', b'C'}))
        self.assertEqual(auth_set_1 & auth_set_2, frozenset({b'B'}))

    def test_mutation(self):
        self.assertEqual(
            structs.Mutation('row', 'cf', 'cq', 'v', 123, 'Value', False),
            (b'row', b'cf', b'cq', b'v', 123, b'Value', False)
        )
        self.assertEqual(
            structs.Mutation(b'row', b'cf', b'cq', b'v', 123, b'Value', True),
            (b'row', b'cf', b'cq', b'v', 123, b'Value', True)
        )
        self.assertEqual(
            structs.Mutation('row'),
            (b'row', b'', b'', b'', None, b'', False)
        )
        self.assertEqual(
            structs.Mutation('row', 'cq'),
            (b'row', b'cq', b'', b'', None, b'', False)
        )
        self.assertEqual(
            structs.Mutation('row', 'cq', 'cf'),
            (b'row', b'cq', b'cf', b'', None, b'', False)
        )
        self.assertEqual(
            structs.Mutation('row', 'cq', 'cf', 'v'),
            (b'row', b'cq', b'cf', b'v', None, b'', False)
        )
        self.assertEqual(
            structs.Mutation(timestamp=123, cq='cq', cf='cf'),
            (b'', b'cf', b'cq', b'', 123, b'', False)
        )
        mutation = structs.Mutation('r', 'cf', 'cq', 'v', 123, 'V', True)
        self.assertEqual(mutation.row_bytes, b'r')
        self.assertEqual(mutation.cf_bytes, b'cf')
        self.assertEqual(mutation.cq_bytes, b'cq')
        self.assertEqual(mutation.visibility_bytes, b'v')
        self.assertEqual(mutation.timestamp, 123)
        self.assertEqual(mutation.value_bytes, b'V')
        self.assertEqual(mutation.delete, True)

    def test_key(self):
        self.assertEqual(
            structs.Key(b'r', 'cf', visibility='v'),
            (b'r', b'cf', None, b'v', None)
        )
        key = structs.Key('r', 'cf', 'cq', 'v', 123)
        self.assertEqual(key.row_bytes, b'r')
        self.assertEqual(key.cf_bytes, b'cf')
        self.assertEqual(key.cq_bytes, b'cq')
        self.assertEqual(key.visibility_bytes, b'v')
        self.assertEqual(key.timestamp, 123)

    def test_range(self):
        self.assertEqual(
            structs.Range(
                start_key=structs.Key('row')
            ),
            (
                (b'row', None, None, None, None),
                True,
                None,
                False
            )
        )
        self.assertEqual(
            structs.Range(
                end_key=structs.Key('row'),
                is_end_key_inclusive=True
            ),
            (
                None,
                True,
                (b'row', None, None, None, None),
                True
            )
        )

    def test_range_exact(self):
        self.assertEqual(
            structs.RangeExact('row'),
            structs.Range(
                start_key=structs.Key('row'),
                end_key=structs.Key('row\x00')
            )
        )
        self.assertEqual(
            structs.RangeExact('row', 'cf'),
            structs.Range(
                start_key=structs.Key('row', 'cf'),
                end_key=structs.Key('row', 'cf\x00')
            )
        )
        self.assertEqual(
            structs.RangeExact('row', 'cf', 'cq'),
            structs.Range(
                start_key=structs.Key('row', 'cf', 'cq'),
                end_key=structs.Key('row', 'cf', 'cq\x00')
            )
        )

    def test_range_prefix(self):
        self.assertEqual(
            structs.RangePrefix('row'),
            structs.Range(
                start_key=structs.Key('row'),
                end_key=structs.Key(b'row\xff'),
                is_end_key_inclusive=True
            )
        )
        self.assertEqual(
            structs.RangePrefix('row', 'cf'),
            structs.Range(
                start_key=structs.Key('row', 'cf'),
                end_key=structs.Key('row', b'cf\xff'),
                is_end_key_inclusive=True
            )
        )
        self.assertEqual(
            structs.RangePrefix('row', 'cf', 'cq'),
            structs.Range(
                start_key=structs.Key('row', 'cf', 'cq'),
                end_key=structs.Key('row', 'cf', b'cq\xff'),
                is_end_key_inclusive=True
            )
        )

    def test_scan_column(self):
        self.assertEqual(structs.ScanColumn('cf', 'cq'), (b'cf', b'cq'))
        self.assertEqual(structs.ScanColumn('cf'), (b'cf', None))
