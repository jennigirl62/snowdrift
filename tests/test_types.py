import unittest

from helpers import col

from snowdrift.types import NormType, compatible, normalize


class NormalizeTests(unittest.TestCase):
    def test_teradata_codes(self):
        self.assertEqual(normalize(col("A", "CV", length=50)), NormType("STRING", length=50))
        self.assertEqual(normalize(col("A", "I8")), NormType("INT", precision=19))
        self.assertEqual(normalize(col("A", "D", precision=10, scale=2)), NormType("DECIMAL", precision=10, scale=2))
        self.assertEqual(normalize(col("A", "DA")).family, "DATE")
        self.assertEqual(normalize(col("A", "CO")).family, "CLOB")

    def test_snowflake_text_and_number(self):
        self.assertEqual(normalize(col("A", "TEXT", length=100)), NormType("STRING", length=100))
        self.assertEqual(normalize(col("A", "NUMBER", precision=12, scale=2)), NormType("DECIMAL", precision=12, scale=2))
        # NUMBER(p,0) is an integer
        self.assertEqual(normalize(col("A", "NUMBER", precision=38, scale=0)), NormType("INT", precision=38))
        self.assertEqual(normalize(col("A", "TIMESTAMP_NTZ")).family, "TIMESTAMP")

    def test_databricks_full_data_type(self):
        self.assertEqual(normalize(col("A", "decimal(10,2)")), NormType("DECIMAL", precision=10, scale=2))
        self.assertEqual(normalize(col("A", "string")), NormType("STRING"))
        self.assertEqual(normalize(col("A", "array<string>")).family, "SEMI")
        self.assertEqual(normalize(col("A", "varchar(20)")), NormType("STRING", length=20))

    def test_integer_ignores_reported_precision(self):
        # Databricks reports INT precision as 32 bits; we compare digits by name instead.
        self.assertEqual(normalize(col("A", "int", precision=32)), NormType("INT", precision=10))

    def test_explicit_zero_scale_is_kept(self):
        self.assertEqual(normalize(col("A", "DECIMAL(18,4)", scale=0, precision=18)), NormType("INT", precision=18))

    def test_unknown_type_is_visible(self):
        self.assertEqual(normalize(col("A", "GEOGRAPHY")).family, "UNKNOWN:GEOGRAPHY")

    def test_compatible(self):
        self.assertTrue(compatible(NormType("CLOB"), NormType("STRING", length=16777216)))
        self.assertFalse(compatible(NormType("STRING"), NormType("INT")))


if __name__ == "__main__":
    unittest.main()
