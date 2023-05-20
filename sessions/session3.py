from collections import Counter
import sys


def tuple_types(input_tuple):
    """ Checks element types in tuple

    param input_tuple: tuple
        object tuple to traverse/type check
    return: tuple
        all element types in tuple format
    """
    return tuple(type(element) for element in input_tuple)


def remove_element(input_tuple, element):
    """ This function drops the specified element

    param input_tuple: tuple
        object to remove element from
    param element: varying
        element of varying type to remove
    return: tuple
        returns tuple without the element specified
    """
    return tuple(item for item in input_tuple if item != element)


def check_containment(input_string, lookup_string):
    """ Function checks if input_string has enough letters to
        construct lookup_string

    param input_string: str
        phrase containing letters available to
        lookup_string
    param lookup_string: str
        phrase to check in input_string counts
    return: bool, bool
        True if input_string has enough letters for lookup_string
        or if lookup_string is physically in input_string as given;
        False otherwise
    """
    # Note: this function finds if there are enough letters
    # in input_string to construct lookup_string and if
    # input_string physically holds lookup_string as given.
    # This is why it returns two booleans rather than one.
    input_string_letter_freq = Counter(input_string)
    lookup_string_letter_freq = Counter(lookup_string)
    enough_letters = True
    for letter, count in lookup_string_letter_freq.items():
        if letter not in input_string_letter_freq.keys() or \
                count > input_string_letter_freq[letter]:
            enough_letters = False
    return enough_letters, lookup_string in input_string


def reverse(input_string):
    """ Reverses input_string

    param input_string: str
        phrase to reverse
    return: str
        returns reversed input_string
    """
    return input_string[::-1]


def concatenate(list1, list2):
    """ Concatenates list1 and list2 by index

    param list1: list
        first list to combine with list2
    param list2: list
        second list to combine with list1
    return: list
        result of index-based concatenation
    """
    # Note: this function assumes equal length lists and each column
    # across both lists have the same element data type amongst
    # str, int, tup, or list
    col_count = len(list1)
    concat_list = []
    for col in range(col_count):
        if type(list1[col]) == int:
            concat_list.append(int(str(list1[col]) + str(list2[col])))
        else:
            concat_list.append(list1[col] + list2[col])
    return concat_list


def concatenate_list_of_lists(input_list):
    """ Concatenates embedded lists of a passed list by index

    param input_list: list of lists
        a list of lists to concatenate
    return: list
        resulting list with index-based concatenation of all
        embedded lists in input_list
    """
    concat_list = input_list[0]
    for idx in range(1, len(input_list)):
        concat_list = concatenate(concat_list, input_list[idx])
    return concat_list


# Note: "_list" was added to function signature to avoid
# confusion with tuple remove_element method.
def remove_element_list(input_list, element):
    """ Removes specified element from input_list

    param input_list: list
        source list
    param element: various
        element to remove from list
    return: list
        resulting list without specified element
    """
    return [item for item in input_list if item != element]


def deep_copy(input_list):
    """ Creates deep copy of input_list

    param input_list: list
        original list to deep copy
    return: list
        new list object with deep copy characteristics
    """
    def copy_by_instance(object_to_copy):
        """ Copies element by its specific container type

        param object_to_copy: varied data type
            embedded element to copy by its internal
            elements
        return: varied data type
            returns copy of object by its
            appropriate original data type
        """
        if isinstance(object_to_copy, dict):
            new_dict = {}
            for k, v in object_to_copy.items():
                new_dict[k] = v
            return new_dict
        elif isinstance(object_to_copy, set):
            new_set = set()
            for item in object_to_copy:
                new_set.add(item)
            return new_set
        elif isinstance(object_to_copy, tuple):
            new_tuple = ()
            for item in object_to_copy:
                new_tuple += (item,)
            return new_tuple
        elif isinstance(object_to_copy, list):
            new_deep_copied_list = []
            for item in object_to_copy:
                new_deep_copied_list.append(item)
            return new_deep_copied_list
        else:
            return f"Unrecognized object type for {object_to_copy}"

    copied_list = []
    primitives = (bool, int, str, float)
    for element in input_list:
        if isinstance(element, primitives):
            copied_list.append(element)
        else:
            copied_list.append(copy_by_instance(element))
    return copied_list


def find(input_dict, specified_key):
    """ Finds all values by key in tuple format

    param input_dict: dict
        dictionary to traverse
    return: tuple
        resulting tuple outlining the values for each key
    """
    # Note: this implementation will include (key, value) pairs
    # even if matched keys have dictionaries as their values.
    # It will then recurse into those embedded dictionaries to find
    # base cases where the values are not dicts. This is for
    # completeness regardless of what value type a matching key
    # holds. This assumes there are no other containers that
    # have dicts embedded in them (e.g., lists with dicts, or
    # tuples with dicts).
    to_return = []
    for key, val in input_dict.items():
        dealing_with_value_as_dict = isinstance(val, dict)
        # Base case
        if not dealing_with_value_as_dict:
            if specified_key == key:
                to_return.append((key, val))
        # Recursive dict_of_dict cases
        elif dealing_with_value_as_dict:
            if specified_key == key:
                to_return.append((key, val))
            found_matching_keys = find(val, specified_key)
            for tuples in found_matching_keys:
                to_return.append(tuples)
    return tuple(to_return)


def min_value(input_dict):
    """ Returns key of minimum value in input_dict

    param input_dict: dict
        dictionary to traverse
    return: various
        returns key corresponding to minimum value
    """
    # Note: if there are multiple minimum values,
    # this implementation will return the first
    # key instance that satisfies the minimum.
    min_key = None
    running_min = sys.maxsize
    for key, val in input_dict.items():
        if val < running_min:
            min_key = key
            running_min = val
    return min_key


if __name__ == '__main__':
    print('Testing tuple_types')
    print(f'Original list: {(1, "hello", [], {}, set())}')
    print(f"Element types: {tuple_types((1, 'hello', [], {}, set()))}")
    print('---------------------------------------------------------------')

    print('Testing remove_element')
    list_to_remove = [1, 2, 3]
    dict_to_remove = {1: 2, 3: 4}
    set_to_remove = (1, 'hello', [], {}, set())
    tuple_to_remove = (5, 6)
    test = (1, 'hello', list_to_remove, dict_to_remove,
            set_to_remove, tuple_to_remove)
    print(f"Original tuple --> {test}\n")
    print(f"Removed 'hello' --> {remove_element(test, 'hello')}")
    print(f"Removed 1 --> {remove_element(test, 1)}")
    print(f"Removed {list_to_remove} --> {remove_element(test, list_to_remove)}")
    print(f"Removed {dict_to_remove} --> {remove_element(test, dict_to_remove)}")
    print(f"Removed {set_to_remove} --> {remove_element(test, set_to_remove)}")
    print(f"Removed {tuple_to_remove} --> {remove_element(test, tuple_to_remove)}")
    print(f"Removed non-existing 'Bert' --> {remove_element(test, 'Bert')}")
    print('---------------------------------------------------------------')

    print('Testing check_containment')
    result_1, result_2 = check_containment('mississippi', 'sipped')
    result_3, result_4 = check_containment('mississippi', 'sipp')
    result_5, result_6 = check_containment('mississippi', 'mississippis')
    result_7, result_8 = check_containment('mississippi', 'ippississim')
    print(f"Is 'sipped' in 'mississippi'? \n\tEnough letters:{result_1}\n"
          f"\tlookup_string exactly in input_string:{result_2}")
    print(f"Is 'sipp' in 'mississippi'? \n\tEnough letters:{result_3}\n"
          f"\tlookup_string exactly in input_string:{result_4}")
    print(f"Is 'mississippis' in 'mississippi'? \n\tEnough letters:{result_5}\n"
          f"\tlookup_string exactly in input_string:{result_6}")
    print(f"Is 'ippississim' in 'mississippi'? \n\tEnough letters:{result_7}\n"
          f"\tlookup_string exactly in input_string:{result_8}")
    print('---------------------------------------------------------------')

    print('Testing reverse')
    test_phrase = "killer robots are too unfriendly"
    print(f'Reversing phrase "{test_phrase}": {reverse("killer robots are too unfriendly")}')
    print(f'Double reversing original phrase: {reverse(reverse("killer robots are too unfriendly"))}')
    print('---------------------------------------------------------------')

    print('Testing concatenate')
    list_a, list_b = [1, 2, 3, 4], [1, 2, 3, 4]
    list_3, list_4 = ["This is", "This is"], [" a test.", " another test."]
    list_5, list_6 = [[1], [3]], [[2], [4]]
    print(f'Concatenating: {list_a} and {list_b}')
    print(f'Result: {concatenate(list_a, list_a)}\n')
    print(f'Concatenating: {list_3} and {list_4}')
    print(f'Result: {concatenate(list_3, list_4)}\n')
    print(f'Concatenating: {list_5} and {list_6}')
    print(f'Result: {concatenate(list_5, list_6)}')
    print('---------------------------------------------------------------')

    print('Testing concatenate_list_of_lists')
    list_of_list1 = [[1, 2, 3, 4],
                     [5, 6, 7, 8],
                     [9, 10, 11, 12]
                     ]
    list_of_list2 = [["This ", "This ", "This "],
                     ["is ", "is ", "is "],
                     ["a ", "another ", "the final "],
                     ["test.", "test.", "test."]
                     ]
    list_of_list3 = [[[1], [3]],
                     [[2], [4]]
                     ]
    list_of_list4 = [[(1,), (3,)],
                     [(2,), (4,)]
                     ]
    list_of_list5 = [[1, "Heterogeneous ", (4,), [[1], [2]]],
                     [2, 'inner elements ', (5,), [[3], [4]]],
                     [3, 'test', (6,), [[5], [6]]]
                     ]
    print(f'Concatenating list of list: {list_of_list1}')
    print(f'Result: {concatenate_list_of_lists(list_of_list1)}\n')
    print(f'Concatenating list of list: {list_of_list2}')
    print(f'Result: {concatenate_list_of_lists(list_of_list2)}\n')
    print(f'Concatenating list of list: {list_of_list3}')
    print(f'Result: {concatenate_list_of_lists(list_of_list3)}\n')
    print(f'Concatenating list of list: {list_of_list4}')
    print(f'Result: {concatenate_list_of_lists(list_of_list4)}\n')
    print(f'Concatenating list of list: {list_of_list5}')
    print(f'Result: {concatenate_list_of_lists(list_of_list5)}')
    print('---------------------------------------------------------------')

    print('Testing remove_element_list')
    test_dct = {}
    dummy_non_existing_elem = set()
    orig_list = [1, 'hello', test_dct, 4]
    print(f"Original List = {orig_list}")
    print(f"Removing 4: {remove_element_list([1, 'hello', test_dct, 4], 4)}")
    print(f"Removing {test_dct}: {remove_element_list([1, 'hello', test_dct, 4], test_dct)}")
    print(f"Removing non-existing {dummy_non_existing_elem}: "
          f"{remove_element_list([1, 'hello', test_dct, 4], dummy_non_existing_elem)}")
    print('---------------------------------------------------------------')

    print('Testing deep_copy')
    test_tuple = (4, 'what what')
    test_dict = {2: 'world'}
    test_set = {1, 2, 4}
    test_inner_tuple = ('test', 'inner tuple')
    test_inner_list = [8, 10, 12, 14, test_inner_tuple]
    test_list = [1, 'string', test_tuple, test_dict, test_set, test_inner_list]
    new_list = deep_copy(test_list)

    print(f'test_list before change: {test_list}')
    print(f'copied_list before change: {new_list}')
    print(f'Does id() match for each element? '
          f'{[id(test_elem) == id(new_elem) for test_elem, new_elem in zip(test_list, new_list)]}')
    test_tuple += ('new tuple element',)
    test_inner_tuple += ('addition to tuple',)
    test_dict[3] = "wide"
    test_set.add(6)
    test_inner_list.append(16)
    print(f'test_list after change: {test_list}')
    print(f'copied_list after change: {new_list}')
    print(f'Does id() match for each element? '
          f'{[id(test_elem) == id(new_elem) for test_elem, new_elem in zip(test_list, new_list)]}')
    print('---------------------------------------------------------------')

    print('Testing find')
    test_dict = {'K1': 2, 'K2': 4,
                 'K3': {'K1': 7,
                        'K2': {'K1': {'K1': 10,
                                      'K2': 11,
                                      'K3': {'K1': 12}},
                               'K2': 13}}}
    print(f'Original dictionary: {test_dict}')
    print(f'Found k, v tuples with k = {"K1"}: {find(test_dict, "K1")}')
    print('---------------------------------------------------------------')

    print('Testing min_value')
    test_dict1 = {'first': 1, 'second': -3243123.045, 'third': 21873649}
    test_dict2 = {'first': 1, 'second': 3243123.045, 'third': 21873649}
    test_dict3 = {'first': -1, 'second': -3243123.045, 'third': -21873649}
    test_dict4 = {1: -1, 'the sec one': -3243123.045, 3: -21873649}
    test_dict5 = {1: -1, 'the sec one': -1, 3: -1}
    print(f'test_dict1:{test_dict1}')
    print(f"Key corresponding to minimum value of test_dict1: {min_value(test_dict1)}\n")
    print(f'test_dict2:{test_dict2}')
    print(f"Key corresponding to minimum value of test_dict2: {min_value(test_dict2)}\n")
    print(f'test_dict3:{test_dict3}')
    print(f"Key corresponding to minimum value of test_dict3: {min_value(test_dict3)}\n")
    print(f'test_dict4:{test_dict4}')
    print(f"Key corresponding to minimum value of test_dict4: {min_value(test_dict4)}\n")
    print(f'test_dict5:{test_dict5}')
    print(f"Key corresponding to minimum value of test_dict4: {min_value(test_dict5)}\n")
