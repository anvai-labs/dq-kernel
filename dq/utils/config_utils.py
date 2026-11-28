# Copyright 2024 Data Quality Framework Contributors
# SPDX-License-Identifier: Apache-2.0

"""Configuration loading and conversion utilities.

A rule pack may be written in HOCON, YAML or JSON. Every supported source
(inline text, a local path, an ``s3://``, ``abfss://`` or ``http(s)://``
URI) and every supported format normalizes to one ``ConfigTree`` here, so
nothing downstream knows which format or source a pack came from
(SPEC-010).

A key is one key in every format. pyhocon reads a key as a path, so a
key holding one of its structural characters has to be kept in quotes
inside a tree (:func:`literal_key` puts them on, :func:`plain_key` takes
them off): a pandera schema's column called ``order.id``, or a logical
dataframe's name.
"""

import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlparse

import requests
from pyhocon import ConfigFactory, ConfigTree

from dq.exceptions import ConfigurationError

#: File extension -> parser, for each supported rule-pack format.
CONFIG_FORMATS = {
    ".conf": "hocon",
    ".hocon": "hocon",
    ".props": "hocon",
    ".properties": "hocon",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".json": "json",
}

_CONFIG_SCHEMES = ("s3", "abfss", "file", "http", "https")

#: What pyhocon reads as structure in a key. A key holding one of these is
#: kept whole in a tree only in quotes -- which is how HOCON quotes a key.
_STRUCTURE_IN_A_KEY = "$}[]:=+#`^?!@*&."


def literal_key(key: Any, where: str = "a rule pack") -> str:
    """Return ``key`` as pyhocon keeps it whole in a tree.

    A JSON or a YAML pack, and a mapping given in code, become a tree
    through ``ConfigFactory.from_dict``, which hands every key to pyhocon
    as a path: a key holding one of pyhocon's structural characters is
    split -- ``a.b`` becomes ``a``, then ``b`` -- or mangled, and nothing
    says so. A pandera schema's column called ``order.id`` arrived as a
    column ``order`` with a property ``id``. In quotes a key is kept as
    one, as a quoted key is in HOCON; :func:`plain_key` takes the quotes
    off again when the tree is read.

    Args:
        key: A key of a JSON or a YAML document, or of a mapping.
        where: What is being read, for the message.

    Returns:
        The key as pyhocon keeps it: itself, or itself in double quotes.

    Raises:
        ConfigurationError: For a key that is not text, that holds a
            structural character and a double quote, or that starts or
            ends with a double quote. pyhocon cannot keep such a key
            whole, and a rule pack is read as it is written or not at all.
    """
    if not isinstance(key, str):
        raise ConfigurationError(f"{where}: every key must be text, got {key!r}")
    if any(character in key for character in _STRUCTURE_IN_A_KEY):
        if '"' in key:
            raise ConfigurationError(
                f"{where}: the key {key!r} holds both a structural character "
                "and a double quote, which cannot be kept as one key"
            )
        return f'"{key}"'
    if key.startswith('"') or key.endswith('"'):
        # pyhocon strips the quotes, or drops the one it cannot pair.
        raise ConfigurationError(
            f"{where}: the key {key!r} starts or ends with a double quote, "
            "which a rule pack reads as the quotes around a key"
        )
    return key


def plain_key(key: str) -> str:
    """Return a key as it was written, without the quotes pyhocon keeps a
    key in a tree.

    pyhocon keeps a key in quotes exactly when it holds one of its
    structural characters: HOCON's ``"a.b"`` is stored with its quotes,
    while ``"a b"`` is stored as it reads. Left as pyhocon gives it, the
    quotes reached whatever read the pack: a schema's column came out
    called ``"order.id"``.
    """
    if len(key) > 1 and key.startswith('"') and key.endswith('"'):
        return key[1:-1]
    return key


def _with_literal_keys(value: Any, where: str) -> Any:
    """Return a JSON or a YAML document with every key spelled so that
    pyhocon keeps it whole."""
    if isinstance(value, dict):
        return {
            literal_key(key, where): _with_literal_keys(item, where)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_with_literal_keys(item, where) for item in value]
    return value


def config_get(config: Any, dotted_key: str, default: Any = None) -> Any:
    """Read a dotted key from a ConfigTree or a plain mapping.

    The framework is handed pyhocon trees in production and plain
    dictionaries in tests and embedders. Trees resolve dotted keys
    themselves; a plain dictionary may store the dotted key as-is or nest
    it, so both spellings are tried.
    """
    if not hasattr(config, "get"):
        return default
    value = config.get(dotted_key, default)
    if value is not default:
        return value
    current = config
    for part in dotted_key.split("."):
        if not hasattr(current, "get") or part not in current:
            return default
        current = current[part]
    return current


def config_format(reference: str) -> str:
    """Return the rule-pack format of ``reference``, by file extension.

    Unknown and missing extensions mean HOCON, the original and default
    format.
    """
    suffix = Path(urlparse(reference).path).suffix.lower()
    return CONFIG_FORMATS.get(suffix, "hocon")


def config_tree_to_python(config: Any) -> Any:
    """Recursively convert a HOCON ConfigTree to native Python types.

    Args:
        config: A ConfigTree, list, dict, or primitive value.

    Returns:
        The equivalent Python dict, list, or primitive.
    """
    if isinstance(config, ConfigTree):
        return config_tree_to_dict(config)
    elif isinstance(config, list):
        return [config_tree_to_python(item) for item in config]
    elif isinstance(config, Mapping):
        return {key: config_tree_to_python(value) for key, value in config.items()}
    else:
        return config


def config_tree_to_dict(config_tree: ConfigTree) -> Dict[str, Any]:
    """Recursively convert a ConfigTree into a regular Python dict.

    A key comes out as it was written: the quotes pyhocon keeps a key in
    a tree are taken off (:func:`plain_key`), so a quoted key in HOCON
    and the same key in JSON or YAML read the same.

    Args:
        config_tree: HOCON ConfigTree instance.

    Returns:
        Plain Python dictionary.
    """
    result = {}
    for key, value in config_tree.items():
        if isinstance(value, ConfigTree):
            said = config_tree_to_dict(value)
        elif isinstance(value, list):
            said = [
                config_tree_to_dict(item) if isinstance(item, ConfigTree) else item
                for item in value
            ]
        else:
            said = value
        result[plain_key(key)] = said
    return result


# ------------------------------------------------------------------- loaders


def load_from_s3(bucket: str, key: str) -> str:
    """Load a configuration file from an S3 bucket.

    Args:
        bucket: S3 bucket name.
        key: Object key path within the bucket.

    Returns:
        File contents as a UTF-8 string.
    """
    import boto3

    session = boto3.session.Session()
    s3 = session.resource("s3")
    res = s3.Object(bucket, key)
    return res.get()["Body"].read().decode("utf-8")


def load_from_adls(uri: str) -> str:
    """Load a configuration file from Azure Data Lake Storage (ADLS Gen2).

    Uses the Hadoop filesystem available through the Spark context
    to read files from ``abfss://`` paths.

    Args:
        uri: Full ADLS URI (abfss://container@account.dfs.core.windows.net/path).

    Returns:
        File contents as a UTF-8 string.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("No active SparkSession found for ADLS access")
    sc = spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    path = sc._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hadoop_conf)
    input_stream = fs.open(path)
    reader = sc._jvm.java.io.BufferedReader(
        sc._jvm.java.io.InputStreamReader(input_stream, "UTF-8")
    )
    lines = []
    line = reader.readLine()
    while line is not None:
        lines.append(line)
        line = reader.readLine()
    reader.close()
    return "\n".join(lines)


def load_from_uri(uri: str) -> str:
    """Load a configuration file from an HTTP/HTTPS URI.

    Args:
        uri: Full URL to fetch.

    Returns:
        Response text.

    Raises:
        ConfigurationError: If the server does not return HTTP 200.
    """
    response = requests.get(uri, timeout=30)
    if response.status_code != 200:
        raise ConfigurationError(
            f"Could not fetch configuration from {uri}: HTTP {response.status_code}"
        )
    return response.text


# ------------------------------------------------------ format-neutral entry


def _concise(error: BaseException) -> str:
    """Shorten a parser error to something a person can read.

    pyhocon failures carry pyparsing's whole grammar -- thousands of
    characters. The useful part is the tail: what was found, and where.
    """
    message = str(error)
    marker = message.rfind(", found ")
    if marker != -1:
        return message[marker + 2 :].replace("\n", " ").strip()
    return message if len(message) <= 240 else message[:240] + "..."


def _hocon_hint(text: str, error: BaseException, source: str) -> str:
    """Return a hint for a parse failure whose cause is a known trap.

    pyhocon reads at most one ``key = value`` per line inside ``{ ... }``.
    Several keys on one line fail in one of two ways, and only those two
    signatures are hinted -- a hint for anything else would be a guess:

    * a second assignment on the line: ``found '='``;
    * a later value of the wrong kind: ``Token '...' ... must be of type``.

    When the text includes another file, the offending line may be there.
    """
    detail = str(error)
    second_assignment = "found '='" in detail
    wrong_kind = "must be of type" in detail and "Token '" in detail
    if not (second_assignment or wrong_kind):
        return ""
    included = re.findall(r'^\s*include\s+"([^"]+)"', text, flags=re.MULTILINE)
    where = f"'{source}'"
    if included:
        where += " and in the included file(s) " + ", ".join(f"'{n}'" for n in included)
    return (
        ". Hint: pyhocon reads one `key = value` per line inside `{ ... }`; "
        f"put each key on its own line in {where}"
    )


def _hocon_error(text: str, error: BaseException, source: str) -> ConfigurationError:
    """Wrap a pyhocon parse failure in a short, actionable message.

    When the cause is known, the message is the position and the fix. Only
    an unrecognized failure falls back to pyhocon's own wording, shortened.
    """
    detail = str(error)
    hint = _hocon_hint(text, error, source)
    position = re.search(r"\(line: \d+, col: \d+\)", detail)
    if hint:
        where = f" at {position.group(0)}" if position else ""
        return ConfigurationError(f"{source} is not valid HOCON{where}{hint}")
    return ConfigurationError(f"{source} is not valid HOCON: {_concise(error)}")


_URI_SCHEME = re.compile(r"^\s*([a-zA-Z][a-zA-Z0-9+.-]*)://")


def _reference_scheme(config: str) -> str:
    """Return the URI scheme of ``config``, or ``""`` when it has none.

    Only ``scheme://`` counts (plus the single-slash ``file:/`` form).
    ``urlparse`` also reports a scheme for text such as ``key: value``,
    which is inline HOCON, not a URI.
    """
    match = _URI_SCHEME.match(config)
    if match:
        return match.group(1).lower()
    if config.lstrip().startswith("file:/"):
        return "file"
    return ""


def _looks_like_inline_hocon(text: str) -> bool:
    """Tell inline HOCON text from a path to a rule-pack file.

    Any HOCON document separates a key from its value, groups with braces,
    or spans lines; a path does none of those.
    """
    return any(
        token in text for token in ("\n", "=", "{", ":")
    ) or text.lstrip().startswith("include ")


def _read_local(path: str) -> str:
    try:
        return Path(path).read_text(encoding="utf-8")
    except OSError as error:
        reason = error.strerror or str(error)
        raise ConfigurationError(
            f"Could not read configuration file '{path}': {reason}"
        ) from error


def _yaml_mapping(text: str) -> Any:
    import yaml  # the core dependency; imported here so the module stays cheap

    return yaml.safe_load(text)


def parse_config_text(
    text: str, fmt: str = "hocon", source: str = "<inline>"
) -> ConfigTree:
    """Parse rule-pack text in any supported format into one ``ConfigTree``.

    Args:
        text: The rule pack's content.
        fmt: ``"hocon"``, ``"yaml"`` or ``"json"``.
        source: Label used in error messages.

    Returns:
        The parsed configuration.

    Raises:
        ConfigurationError: If the text is not valid in ``fmt``, or a YAML
            or JSON pack is not a mapping.
    """
    if fmt not in ("hocon", "yaml", "json"):
        raise ConfigurationError(f"Unsupported configuration format {fmt!r}")
    if fmt == "hocon":
        try:
            return ConfigFactory.parse_string(text)
        except ConfigurationError:
            raise
        except Exception as error:  # noqa: BLE001 - pyparsing raises its own types
            raise _hocon_error(text, error, source) from error
    try:
        data = json.loads(text) if fmt == "json" else _yaml_mapping(text)
    except ConfigurationError:
        raise
    except Exception as error:  # noqa: BLE001 - each parser raises its own type
        raise ConfigurationError(
            f"{source} is not valid {fmt.upper()}: {error}"
        ) from error
    if not isinstance(data, dict):
        raise ConfigurationError(
            f"{source}: a {fmt.upper()} rule pack must be a mapping with a top-level "
            "'dqframework' key"
        )
    return ConfigFactory.from_dict(_with_literal_keys(data, source))


def load_config(config: Any) -> ConfigTree:
    """Load a rule pack from inline text, a path, or a URI, in any format.

    Args:
        config: A ``ConfigTree``, a mapping, or a string that is inline
            HOCON, a local path (with or without ``file://``), or an
            ``s3://``, ``abfss://`` or ``http(s)://`` URI. The format
            follows the file extension: ``.conf``/``.hocon`` (the
            default), ``.yaml``/``.yml``, or ``.json``.

    Returns:
        The parsed configuration.

    Raises:
        ConfigurationError: For an empty input, an unsupported scheme, an
            unreadable source, or text that does not parse.
    """
    if isinstance(config, ConfigTree):
        return config
    if isinstance(config, Mapping):
        return ConfigFactory.from_dict(_with_literal_keys(dict(config), "the mapping"))
    if isinstance(config, os.PathLike):
        config = os.fspath(config)
    if not isinstance(config, str) or not config.strip():
        raise ConfigurationError(
            "Configuration must be a non-empty string, a path, a mapping, or a ConfigTree"
        )
    scheme = _reference_scheme(config)
    parsed = urlparse(config)
    if scheme and scheme not in _CONFIG_SCHEMES:
        raise ConfigurationError(f"Unsupported config scheme: {scheme}")

    # Only a scheme-less string can be inline HOCON.
    if scheme == "file" or (scheme == "" and not _looks_like_inline_hocon(config)):
        if scheme != "file":
            path = config
        else:
            path = (
                config[len("file://") :] if config.startswith("file://") else config[5:]
            )
        fmt = config_format(path)
        if fmt == "hocon":
            # parse_file resolves `include` relative to the file's directory.
            try:
                return ConfigFactory.parse_file(path)
            except ConfigurationError:
                raise
            except Exception as error:  # noqa: BLE001 - pyparsing/OSError types
                if not os.path.exists(path):
                    raise ConfigurationError(
                        f"Configuration file '{path}' does not exist"
                    ) from error
                raise _hocon_error(
                    Path(path).read_text(encoding="utf-8"), error, path
                ) from error
        text = _read_local(path)
        return parse_config_text(text, fmt, source=path)

    if scheme:  # remote rule pack
        try:
            text = _read_remote(config, scheme, parsed)
        except ConfigurationError:
            raise
        except Exception as error:  # noqa: BLE001 - each client raises its own type
            raise ConfigurationError(
                f"Failed to load configuration from {config}: {error}"
            ) from error
        return parse_config_text(text, config_format(config), source=config)
    return parse_config_text(config, "hocon", source="<inline>")


def _read_remote(config: str, scheme: str, parsed) -> str:
    """Fetch rule-pack text from a remote source by scheme."""
    if scheme == "s3":
        return load_from_s3(bucket=parsed.netloc, key=parsed.path)
    if scheme == "abfss":
        return load_from_adls(config)
    return load_from_uri(config)
