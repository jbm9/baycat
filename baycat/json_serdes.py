from abc import ABC, abstractmethod
import json
import logging


class JSONSerDes:
    '''A base class that adds hooks for JSON de/serialization'''

    JSON_CLASSNAME = "JSONSerDes"

    def __init__(self):
        self._json_classname = JSON_CLASSNAME

    def copy(self):
        '''Does a value copy of our object

        This does a trivial/naive JSON round-trip to get a copy of
        the object.
        '''
        return self.__class__.from_json_obj(self.to_json_obj())

    @abstractmethod
    def to_json_obj(self):
        return vars(self)

    @abstractmethod
    def to_json(self):
        return json.dumps(self.to_json_obj())

    @classmethod
    @abstractmethod
    def from_json_obj(cls, json_obj):
        result = cls(json_obj)
        if not result.entries:
            return result

        if result.entries.values()[0].__class__ == dict:
            raise ValueError("Got partial decode")

        return result


class BaycatJSONEncoder(json.JSONEncoder):
    @staticmethod
    def default(obj):
        if isinstance(obj, JSONSerDes):
            return obj.to_json_obj()
        return json.JSONEncoder.default(self, obj)


DECODERS = {}


def baycat_json_decoder(obj):
    try:
        clsname = obj["_json_classname"]
    except KeyError:
        # not one of ours
        return obj

    if not DECODERS:
        for cls in JSONSerDes.__subclasses__():
            DECODERS[cls.JSON_CLASSNAME] = cls.from_json_obj

    if clsname not in DECODERS:
        # XXX TODO Add test coverage for this branch
        logging.error(f'Got unknown JSON classname: {clsname}')
        logging.debug(f'Known JSON decoders: {DECODERS}')
        raise ValueError(f'Got unknown JSON classname: {clsname}')

    return DECODERS[clsname](obj)
