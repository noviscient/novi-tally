from typing import Any, Literal

import tomllib

from novi_tally.connections import file_systems as fss
from novi_tally.connections.openfigi import OpenFigiApi
from novi_tally.connections.formidium import FormidiumApi


def load_config(filepath: str) -> dict[str, Any]:
    with open(filepath, "rb") as f:
        config = tomllib.load(f)

    return parse_config(config)


def make_file_system(
    fs_type: Literal["S3", "SFTP"], kwargs: dict[str, str]
) -> fss.RemoteFileSystem:
    if fs_type == "S3":
        fs_cls = fss.S3FileSystem
    elif fs_type == "SFTP":
        fs_cls = fss.SftpFileSystem
    else:
        raise ValueError(f"Unknown file system type: {fs_type}")

    return fs_cls(**kwargs)


def parse_connection(
    connection_type: str, kwargs: dict[str, str]
) -> fss.RemoteFileSystem | OpenFigiApi | FormidiumApi:
    category, subtype = connection_type.split(".")
    if category == "FileSystem":
        return make_file_system(fs_type=subtype, kwargs=kwargs)  # type: ignore

    if subtype == "OPENFIGI":
        return OpenFigiApi(**kwargs)

    if subtype == "FORMIDIUM":
        return FormidiumApi(**kwargs)

    raise ValueError(f"Unknown connection type: {connection_type}")


def parse_config(config: dict[str, Any]) -> dict[str, Any]:
    # parse connections
    connections = {}
    for c_name, c_config in config["connection"].items():
        connections[c_name] = parse_connection(
            connection_type=c_config["type"], kwargs=c_config["kwargs"]
        )

    # parse dataloader kwargs
    dataloader_kwargs = {}
    for p_name, p_config in config["provider"].items():
        p_kwargs = {}
        for k, v in p_config.items():
            if v["type"] == "connection":
                p_kwargs[k] = connections[v["name"]]
            else:
                p_kwargs[k] = v

        dataloader_kwargs[p_name] = p_kwargs

    return dataloader_kwargs
