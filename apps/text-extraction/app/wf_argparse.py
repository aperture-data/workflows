import argparse
import os
import logging
from typing import List, Optional

# TODO: Elevate this to a shared library
# This class wraps argparse and adds support for environment variables.
# Each argument automatically defaults to being set from a WF_ environment variable.
# The environment variable name is derived from the argument name by removing
# dashes and converting to uppercase, e.g. --crawl becomes WF_CRAWL.
# In legacy mode, if the `WF_` envar is not present, it will also check for the
# legacy envar name (without the `WF_` prefix).
# The intention of using the `WF_` prefix is to allow the code to detect
# # misspellings or unsupported parameters.
# If the environment variable is not set, the default value is used.

logger = logging.getLogger(__name__)


class ArgumentParser:
    def __init__(self, *args, support_legacy_envars=False, **kwargs):
        self.parser = argparse.ArgumentParser(*args, **kwargs)
        self.envars_used = set()
        self.support_legacy_envars = support_legacy_envars

    def add_argument(self, *names, required=False, type=None, default=None, sep=None,
                     **kwargs):
        if type is bool:
            type = self.str2bool
        action = 'append' if sep else None
        name = names[0]
        default = self.get_default(name, default, sep)
        required = required and default is None

        self.parser.add_argument(
            *names, type=type, default=default, action=action, required=required, **kwargs)

    def _envar_name(self, name: str):
        while name.startswith('-'):
            name = name[1:]
        name = name.upper().replace('-', '_')
        return "WF_" + name

    def get_default(self, name: str, default=None, sep=None):
        envar_name = self._envar_name(name)
        self.envars_used.add(envar_name)
        legacy_envar_name = envar_name[3:]  # remove WF_
        logger.debug(
            f"name: {name}, envar_name: {envar_name}, legacy_envar_name: {legacy_envar_name}")
        if envar_name in os.environ:
            logging.info(f"Using {envar_name} from environment")
            result = os.environ.get(envar_name)
        elif self.support_legacy_envars and legacy_envar_name in os.environ:
            logging.info(f"Using legacy {legacy_envar_name} from environment")
            result = os.environ.get(legacy_envar_name)
        else:
            result = default
        if sep and result is not None:
            return result.split(sep)
        return result

    @staticmethod
    def str2bool(v):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    def check_envars(self):
        unused_envars = set()
        for envar in os.environ:
            if envar.startswith("WF_") and envar not in self.envars_used:
                logging.error(f"Environment variable {envar} is not used.")
                unused_envars.add(envar)
        assert not unused_envars, "Unused environment variables found: " + \
            ", ".join(unused_envars)

    def parse_args(self, *args, **kwargs):
        self.check_envars()
        result = self.parser.parse_args(*args, **kwargs)
        return result

    # proxy additional methods to self.parser
    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        if hasattr(self.parser, name):
            return getattr(self.parser, name)
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{name}'"
        )
