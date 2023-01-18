from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from createmetrics.graph.SumDebts import *
import createmetrics.config.ConfigStore as ConfigStore


class SumDebtsTest(BaseTestCase):

    def test_unit_test_0(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/createmetrics/graph/SumDebts/in0/schema.json',
            'test/resources/data/createmetrics/graph/SumDebts/in0/data/test_unit_test_0.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/createmetrics/graph/SumDebts/out/schema.json',
            'test/resources/data/createmetrics/graph/SumDebts/out/data/test_unit_test_0.json',
            'out'
        )
        dfOutComputed = SumDebts(self.spark, dfIn0)
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
