import enum
import datetime

import peewee

from weathermap import config


db = peewee.PostgresqlDatabase(config.db_name, user=config.db_user, host=config.db_host, password=config.db_password)


@enum.unique
class MeasurementType(enum.IntEnum):
    UDP_SPEED = 1

    @classmethod
    def as_choices(cls):
        return [(attr.value, attr.name) for attr in cls]


class BaseModel(peewee.Model):
    class Meta:
        database = db


class Measurement(BaseModel):
    when = peewee.DateTimeField(default=datetime.datetime.now)
    src_node = peewee.CharField()
    dest_node = peewee.CharField()
    type = peewee.IntegerField(choices=MeasurementType.as_choices())
    value = peewee.BigIntegerField()


def create_tables():
    db.create_tables([Measurement], safe=True)
