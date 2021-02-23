import functools

import aiobotocore

# https://aiobotocore.readthedocs.io

session = aiobotocore.get_session()


def auto_async_client(*create_client_args, **create_client_kwargs):
    """
        First argument of decorated function must be ``client``.
        Omit this argument when you call decorated function.
    """
    def decorator_auto_async_client(func):
        @functools.wraps(func)
        async def wrapper_auto_async_client(*args, **kwargs):
            async with session.create_client(*create_client_args, **create_client_kwargs) as client:
                return await func(client, *args, **kwargs)

        return wrapper_auto_async_client

    return decorator_auto_async_client


auto_s3_async_client = auto_async_client("s3")
"""
    First argument of decorated function must be ``client``.
    Omit this argument when you call decorated function.
"""
