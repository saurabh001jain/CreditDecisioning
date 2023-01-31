from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, Year: int=None):
        self.spark = None
        self.update(Year)

    def update(self, Year: int=2018):
        self.Year = self.get_int_value(Year)
        pass
