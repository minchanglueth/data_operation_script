import glob
import logging.config
import os

import yaml
import sentry_sdk
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

# Import environment when there are local .env files.
if local_env_file := os.getenv("LOCAL_ENV_FILE"):
    local_config_files = [local_env_file]
else:
    local_config_files = glob.iglob("local.*.env")

for local_config_file in glob.iglob("local.*.env"):
    logging.info(f'Importing environment from "{local_config_file}".')
    for line in open(local_config_file):
        clean_line = line.strip()
        eq_idx = clean_line.find("=")
        if 0 < eq_idx < len(clean_line) - 1:
            os.environ[clean_line[:eq_idx]] = clean_line[eq_idx + 1 :]  # noqa

sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    integrations=[
        CeleryIntegration(),
        SqlalchemyIntegration(),
        AioHttpIntegration(),
        RedisIntegration(),
    ],
)


def getenv_boolean(var_name: str, default_value: bool = False) -> bool:
    result = default_value
    env_value = os.getenv(var_name)
    if env_value is not None:
        result = env_value.upper() in ("TRUE", "1")
    return result


def getenv_int(var_name: str, default_value: int = 0) -> int:
    result = default_value
    env_value = os.getenv(var_name)
    if env_value:
        try:
            result = int(env_value)
        except ValueError:
            result = default_value
    return result


class BaseConfig:
    """
    Loads general.env and common.env
    """

    DOMAIN = os.getenv("DOMAIN")

    LOGGING_CONFIG_FILE = os.getenv("LOGGING_CONFIG_FILE")


with open(BaseConfig.LOGGING_CONFIG_FILE, "r") as file:
    config = yaml.safe_load(file.read())
    logging.config.dictConfig(config)
