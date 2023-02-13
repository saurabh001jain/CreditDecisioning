from prophecy.config import ConfigBase


class Config(ConfigBase):

    def __init__(self, Year: int=None, user_email: str=None):
        self.spark = None
        self.update(Year, user_email)

    def update(self, Year: int=2018, user_email: str="sparklearner123@gmail.com"):
        self.Year = self.get_int_value(Year)
        self.user_email = user_email
        pass
