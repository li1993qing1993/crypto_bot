import errno
import json
import logging
import os
import signal
import time
from functools import wraps

logger = logging.getLogger()

MAX_RETRY = 3


def call_api_with_retry(fun, *args, **kwargs):
    """
    Retry API call with exponential back off
    :param fun: function to call
    :param args: arguments of this function
    :param kwargs: key argument of this fuction
    :return:
    """
    for i in range(MAX_RETRY):
        try:
            return fun(*args, **kwargs)
        except Exception as e:
            logger.error('An unexpected error happened while calling %s', fun.__name__)
            logger.error(e)
        exponential_delay(0.2, i)
    # Final attempt.
    return fun(*args, **kwargs)


def exponential_delay(coeff, i):
    time.sleep(coeff * (2 ** i))


def timeout(seconds=10, error_message=os.strerror(errno.ETIME)):
    def decorator(func):
        def _handle_timeout(signum, frame):
            raise TimeoutError(error_message)

        def wrapper(*args, **kwargs):
            signal.signal(signal.SIGALRM, _handle_timeout)
            signal.alarm(seconds)
            try:
                result = func(*args, **kwargs)
            finally:
                signal.alarm(0)
            return result

        return wraps(func)(wrapper)

    return decorator


def write_credentials(credentials, filename):
    if os.path.isfile(filename):
        with open(filename, 'w') as f:
            json.dump(credentials, f, indent=2)


def get_credentials(filename):
    if os.path.isfile(filename):
        with open(filename, 'r') as f:
            return json.load(f)


def log_json_utils(log_fun, **kwargs):
    log_fun(json.dumps(kwargs))


def acquire_semaphore(semaphore, timeout=60):
    logger.info("Acquiring a semaphore in order to make create/delete order operations.")
    semaphore_status = semaphore.acquire(timeout=timeout)
    if semaphore_status:
        logger.info("Successfully acquired a semaphore.")
    else:
        logger.error("The semaphore acquirement timed out. Bypassing semaphore.")


def release_semaphore(semaphore):
    logger.info("Releasing a semaphore.")
    try:
        semaphore.release()
    except ValueError as e:
        logger.error(e)

