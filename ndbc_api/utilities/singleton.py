"""
A metaclass for singleton types.
"""


class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton,
                                        cls).__call__(*args, **kwargs)
        elif (args or kwargs) and hasattr(cls._instances[cls], '_reconfigure'):
            cls._instances[cls]._reconfigure(*args, **kwargs)
        return cls._instances[cls]
