# pylint: disable=unused-argument, no-value-for-parameter

from unittest import TestCase

from radical.pilot.utils import serializer


# ------------------------------------------------------------------------------
#
class TestSerializer(TestCase):

    # --------------------------------------------------------------------------
    #
    def test_ser_path(self):

        def hello_test():
            return 1

        obj = hello_test
        ser_obj_file_path = serializer.serialize_file(obj)
        expected_path = '/tmp/rp_obj.pkl'

        self.assertEqual(ser_obj_file_path, expected_path)


    # --------------------------------------------------------------------------
    #
    def test_ser_obj(self):

        def hello_test():
            return 1

        obj = hello_test
        ser_obj_byte = serializer.serialize_obj(obj)

        self.assertIsInstance(ser_obj_byte, bytes)


    # --------------------------------------------------------------------------
    #
    def test_ser_bson(self):

        obj = {'func': bytes(100), 'args': (), 'kwargs': {}}

        ser_obj_bson_str = serializer.serialize_bson(obj)

        self.assertIsInstance(ser_obj_bson_str, str)


    # --------------------------------------------------------------------------
    #
    def test_dser_bson(self):

        # we fail if we recive byte
        not_str_ser_obj = bytes(100)
        with self.assertRaises(Exception):
            serializer.deserialize_bson(not_str_ser_obj)

        # we pass if we have str
        obj = {'func': bytes(100), 'args': (), 'kwargs': {}}
        ser_obj_bson_str  = serializer.serialize_bson(obj)
        dser_obj_bson_str = serializer.deserialize_bson(ser_obj_bson_str)

        self.assertIsInstance(dser_obj_bson_str, dict)


    # --------------------------------------------------------------------------
    #
    def test_dser_obj(self):

        # callable
        def hello_test():
            return 1

        # non-callable (module)
        import sys

        list_obj = [sys, hello_test]

        dict_obj = {'non-callable': sys, 'callable': hello_test}

        supported_types = [hello_test, sys, list_obj, dict_obj]

        for obj in supported_types:

            ser_byte = serializer.serialize_obj(obj)
            self.assertIsInstance(ser_byte, bytes)

            desr_byte = serializer.deserialize_obj(ser_byte)
            self.assertIsInstance(desr_byte, type(obj))


# ------------------------------------------------------------------------------

