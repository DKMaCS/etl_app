import argparse
from types import SimpleNamespace as Namespace


def convert_namespace(obj):
    """ Converts any Namespace objects to dict
    param obj: Namespace or dict
        object requiring conversion from Namespace to dict
    return: dict
        object in dict form
    """
    if isinstance(obj, argparse.Namespace):
        return vars(obj)
    elif isinstance(obj, dict):
        return convert_embedded_namespace_to_dict(obj)


def convert_embedded_namespace_to_dict(obj):
    """ Finds and converts embedded namespace objects in obj
    param obj: dict
        the dict object to check for embedded Namespace objects
    return: dict
        returns dict with only embedded dicts
    """
    for key, value in obj.items():
        if isinstance(value, Namespace):
            obj[key] = vars(value)

    return obj