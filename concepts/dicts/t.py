
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, NoneStr


# class Model(BaseModel):
#
#     age          : int
#     first_name   : str                = 'John'
#     last_name    : NoneStr            = None
#     signup_ts    : Optional[datetime] = None
#     list_of_ints : List[int]
#
#
# m = Model(age=42, list_of_ints=[1, '2', b'3'])
# m.age = True
# # print(m.middle_name)  # not a model field!
# Model()


from good.voluptuous import *
print(Schema({
    'name': str,
    'age': lambda v: int(v)
})({
    'name': 'Alex',
    'age': '18',
}))

