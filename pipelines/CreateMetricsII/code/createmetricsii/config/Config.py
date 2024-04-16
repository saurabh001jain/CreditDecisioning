from createmetricsii.graph.Encryption.config.Config import SubgraphConfig as Encryption_Config
from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, Year: int=None, user_email: str=None, database_name: str=None, Encryption: dict=None, **kwargs):
        self.spark = None
        self.update(Year, user_email, database_name, Encryption)

    def update(
            self,
            Year: int=2018,
            user_email: str="sparklearner123@gmail.com",
            database_name: str="sparklearnerdev",
            Encryption: dict={},
            **kwargs
    ):
        prophecy_spark = self.spark
        self.Year = self.get_int_value(Year)
        self.user_email = user_email
        self.database_name = database_name
        self.Encryption = self.get_config_object(
            prophecy_spark, 
            Encryption_Config(prophecy_spark = prophecy_spark), 
            Encryption, 
            Encryption_Config
        )
        pass
