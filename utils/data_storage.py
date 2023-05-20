from abc import ABC, abstractmethod


class DataStorage(ABC):
    """ Abstract class (ABC) to expose read and write methods
        to children classes

        Main behaviors covered are reading files to memory and
        writing dataframes to files using appropriate
        json configuration structures
    """

    _count = 0

    def __init__(self):
        """ Initialization to save an instance count and
            its address in memory
        """
        DataStorage._count += 1
        self._instanceCount = DataStorage._count
        self._id = hex(id(self))

    @abstractmethod
    def read(self, **kwargs):
        """ Abstract method to read local files into memory

        params **kwargs: dict
            key-word args in map form
        returns: various
            returned object depends on use case
        """
        ...

    @abstractmethod
    def write(self, **kwargs):
        """ Abstract method to write objects to local memory

        params **kwargs: dict
            key-word args in map form
        returns: n/a
            writes to memory and returns control to caller
        """
        ...
