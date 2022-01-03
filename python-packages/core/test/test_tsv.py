import unittest
from omigo_core import tsv

class TestTSV(unittest.TestCase):
    def create_tsv1(self):
        return tsv.TSV("name",["x"])

    def test_select1(self):
        xtsv = self.create_tsv1()
        self.assertEqual(xtsv.select("name").columns(), ["name"])

    def test_select2(self):
        xtsv = self.create_tsv1()
        self.assertEqual(xtsv.select("n.*").columns(), ["name"])

if __name__ == '__main__':
    unittest.main()
