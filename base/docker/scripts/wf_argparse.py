import argparse
import os
import logging
import re
from urllib.parse import urlparse, urlunparse, ParseResult
import ipaddress


# This class wraps argparse and adds a number of additional features:
# 
# 1. Support for environment variables.
# Each argument automatically defaults to being set from a WF_ environment variable.
# The environment variable name is derived from the argument name by removing
# dashes and converting to uppercase, e.g. --crawl becomes WF_CRAWL.
# In legacy mode, if the `WF_` envar is not present, it will also check for the
# legacy envar name (without the `WF_` prefix).
# The intention of using the `WF_` prefix is to allow the code to detect
# # misspellings or unsupported parameters.
# If the environment variable is not set, the default value is used.
#
# 2. Fix bool from string
# Boolean values can be specified as strings like "true" or "false".
#
# 3. Support for lists
# argparse already supports lists by repeated argument,
# but this class adds support for separators in the environment variable,
# using the "sep" parameter which can be either a string or a regex.
#
# 4. Registry for type sanitation, validation, conversion
# This class provides a registry for type sanitation, validation, and conversion.
# If the type is specified as a string, it is looked up in the registry.
#
# 5. CLI
# This module also provides a CLI interface for the registry
# which can be used to sanitize and validate untrusted input in bash scripts.



# The WF_ prefix is also used by other parts of the workflow infrastructure,
# so we need to ignore some of them.

ENVAR_IGNORE_LIST = {"WF_LOGS_AWS_BUCKET", "WF_LOGS_AWS_CREDENTIALS", }

SEP_COMMA = ","
SEP_WHITESPACE = re.compile(r"\s+")

logger = logging.getLogger(__name__)


class ArgumentParser:
    def __init__(self, *args, support_legacy_envars=False, **kwargs):
        self.parser = argparse.ArgumentParser(*args, **kwargs)
        self.envars_used = set()
        self.support_legacy_envars = support_legacy_envars

    def add_argument(self, *names, required=False, type=None, default=None, sep=None,
                     **kwargs):
        if type is bool:
            type = "bool"
        elif type in (str, None):
            type = self.trim

        if isinstance(type, str):
            assert type in VALIDATORS, f"Unknown type: {type}"
            type = VALIDATORS[type]

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
            result = os.environ.get(envar_name)
            logging.info(f"Using {envar_name} from environment -> {result}")
        elif self.support_legacy_envars and legacy_envar_name in os.environ:
            result = os.environ.get(legacy_envar_name)
            logging.info(
                f"Using legacy {legacy_envar_name} from environment -> {result}")
        else:
            result = default
        if sep and result is not None:
            if isinstance(sep, str):
                return result.split(sep)
            elif isinstance(sep, re.Pattern):
                return sep.split(result)
            else:
                raise ValueError(f"Invalid separator: {sep}")

        return result

    @staticmethod
    def trim(s):
        return s.strip()

    def check_envars(self):
        unused_envars = set()
        for envar in os.environ:
            if envar.startswith("WF_") \
                and envar not in self.envars_used \
                    and envar not in ENVAR_IGNORE_LIST:
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

# Validator functions perform various operations on the value:
# 1. Sanitize the value, for example strip and upper case it
# 2. Validate the value, for example check if it is a valid log level
# 3. Convert the value into an appropriate Pythonic type
#
# If validation fails, then an ArgumentTypeError is raised.
# If cli is True, then the return value is either a string or a stringifiable type.

def validate_log_level(v, *, cli=False):
    """
    Checks a logging level.
    Expects a string like "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL".
    Returns a numeric code.
    """
    v = v.strip().upper()
    # Canonicalize against logging module
    level = logging.getLevelName(v)
    # logging.getLevelName() is bi-directional: int -> str, str -> int
    if isinstance(level, str):
        # Unknown level string
        raise argparse.ArgumentTypeError(f"Unknown log level: {v}")
    if cli:
        return logging.getLevelName(level)  # return canonical name (string)
    return level  # return numeric code


def validate_bool(v, *, cli=False):
    """
    Checks a boolean value.
    Expects a string like "true", "false".
    Returns a boolean value.
    """
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        result = True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        result = False
    else:
        raise argparse.ArgumentTypeError(f"Boolean value expected: {v}")
    if cli:
        return "true" if result else "false" # JSON
    return result


class URL(ParseResult):
    """
    A wrapper around urllib.parse.ParseResult that provides a useful string representation.
    """
    def __str__(self):
        return urlunparse(self)


def validate_web_url(v, *, cli=False):
    """
    Checks a web URL.
    Expects a string like "https://www.example.com".
    Returns a URL.
    """
    v = v.strip()
    parsed = URL(urlparse(v))

    if not parsed.scheme or not parsed.netloc:
        raise argparse.ArgumentTypeError(f"Invalid URL: {v}")

    # Sanitize the scheme to lowercase
    parsed = parsed._replace(scheme=parsed.scheme.lower())

    if parsed.scheme not in ("http", "https"):
        raise argparse.ArgumentTypeError(f"Invalid URL scheme: {v}")

    host = parsed.hostname
    if not host:
        raise argparse.ArgumentTypeError(f"No hostname: {v}")

    parsed = parsed._replace(hostname=validate_hostname(host, cli=True))

    # TODO: Consider checking for internal domains that might expose sensitive information,
    # especially in the context of cloud hosting.

    if parsed.port:
        parsed = parsed._replace(port=validate_int_in_range(parsed.port, min=1, max=65535))

    # TODO: Consider sanitizing query parameters to prevent injection attacks.

    return parsed


def validate_hostname(v, *, cli=False):
    """
    Checks a hostname.
    Expects a string like "www.example.com".
    Returns a hostname.
    """
    v = v.strip()
    try:
        ip = ipaddress.ip_address(v)
    except ValueError:
        try:
            v.encode("idna")
        except Exception:
            raise argparse.ArgumentTypeError(f"Invalid hostname: {v}")



INT_RE = re.compile(r"^[+-]?\d+$")

def validate_int_in_range(v, *, cli=False, min=None, max=None):
    """
    Checks an integer in a range.
    Expects a string like "123".
    Returns an integer.
    """
    if isinstance(v, int):
        value = v
    else:
        v = v.strip()
        if not INT_RE.match(v):
            raise argparse.ArgumentTypeError(f"Invalid integer: {v}")
        value = int(v)

    if min is not None and value < min:
        raise argparse.ArgumentTypeError(f"Value must be greater than or equal to {min}: {v}")
    if max is not None and value > max:
        raise argparse.ArgumentTypeError(f"Value must be less than or equal to {max}: {v}")
    return str(value) if cli else value


FLOAT_RE = re.compile(r"^[+-]?\d+(\.\d+)?$")

def validate_float_in_range(v, *, cli=False, min=None, max=None):
    """
    Checks a float in a range.
    Expects a string like "123.45".
    Returns a float.
    """
    if isinstance(v, float):
        value = v
    else:
        v = v.strip()
        if not FLOAT_RE.match(v):
            raise argparse.ArgumentTypeError(f"Invalid float: {v}")
        value = float(v)
    if min is not None and value < min:
        raise argparse.ArgumentTypeError(f"Value must be greater than or equal to {min}: {v}")
    if max is not None and value > max:
        raise argparse.ArgumentTypeError(f"Value must be less than or equal to {max}: {v}")
    return str(value) if cli else value

VALIDATORS = {
    "log_level": validate_log_level,
    "bool": validate_bool,
    "web_url": validate_web_url,
    "positive_int": lambda v, *, cli=False: validate_int_in_range(v, cli=cli, min=1),
    "non_negative_int": lambda v, *, cli=False: validate_int_in_range(v, cli=cli, min=0),
    "port": lambda v, *, cli=False: validate_int_in_range(v, cli=cli, min=1, max=65535),
    'non_negative_float': lambda v, *, cli=False: validate_float_in_range(v, cli=cli, min=0),
}