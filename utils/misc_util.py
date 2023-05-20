from argparse import Namespace as ArgNamespace
from types import SimpleNamespace


def convert_namespace_to_dict(mapping):
    """ Recursively convert given mapping of type SimpleNamespace or argparse.Namespace into dict

    param mapping: SimpleNamespace or argparse.Namespace; Provided mapping
    return: dict; Converted mapping of type dict
    """
    if isinstance(mapping, SimpleNamespace) or isinstance(mapping, ArgNamespace):
        mapping_target = vars(mapping)
    else:
        mapping_target = mapping

    # Make recursive call.
    if isinstance(mapping_target, dict):
        for (key, value) in mapping_target.items():
            mapping_target[key] = convert_namespace_to_dict(value)

    return mapping_target


def eval_elem_mapping(mapping, key, default_value=None):
    """ Evaluate given mapping and returns value element, based on provided key

    param mapping: dict; Provided mapping
    param key: int or str; Provided key
    param default_value: int or str; default=None; Default value
    return: Resulted mapping value
    """
    value_target = mapping[key] if isinstance(mapping, dict) and key in mapping else default_value
    return value_target if value_target else default_value


def eval_update_mapping(mapping, key, update_with):
    """ Evaluate given mapping and if an element of dict type exists,
        update it with provided parameter.

    param mapping: dict; Provided mapping
    param key: int or str; Provided key
    param update_with: dict; Provided dict to update with
    return: Resulted mapping
    """
    mapping_target = eval_elem_mapping(mapping, key, dict())
    if mapping_target:
        if isinstance(mapping_target, dict) and update_with and isinstance(update_with, dict):
            mapping_target.update(update_with)
    return mapping_target
