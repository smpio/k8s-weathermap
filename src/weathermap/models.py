import datetime

import peewee


db = peewee.PostgresqlDatabase('postgres')


class BaseModel(peewee.Model):
    class Meta:
        database = db


class Measure(BaseModel):
    when = peewee.DateTimeField(default=datetime.datetime.now)
    src_node = peewee.CharField()
    dest_node = peewee.CharField()
    type = peewee.IntegerField()    #
    value = peewee.BigIntegerField()
