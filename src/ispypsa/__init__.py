import pandas as pd

# pandas options
try:
    pd.set_option("future.no_silent_downcasting", True)
except pd.errors.OptionError:
    pass


__all__ = []
