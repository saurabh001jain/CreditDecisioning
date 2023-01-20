from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from createmetrics.graph.DebtsMonthly import *
import createmetrics.config.ConfigStore as ConfigStore


class DebtsMonthlyTest(BaseTestCase):

    def test_converttomonthly(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/createmetrics/graph/DebtsMonthly/in0/schema.json',
            'test/resources/data/createmetrics/graph/DebtsMonthly/in0/data/test_converttomonthly.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/createmetrics/graph/DebtsMonthly/out/schema.json',
            'test/resources/data/createmetrics/graph/DebtsMonthly/out/data/test_converttomonthly.json',
            'out'
        )
        dfOutComputed = DebtsMonthly(self.spark, dfIn0)
        assertDFEquals(
            dfOut.select("name", "income", "debt"),
            dfOutComputed.select("name", "income", "debt"),
            self.maxUnequalRowsToShow
        )

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        ConfigStore.Utils.initializeFromArgs(
            self.spark,
            Namespace(file = f"configs/resources/config/{fabricName}.json", config = None)
        )
