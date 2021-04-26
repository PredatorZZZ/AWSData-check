import logging


class LogInfo:

    def __init__(self, name='Logger', level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        sh = logging.StreamHandler()
        fmt = '%(asctime)s - [%(levelname)s] - %(message)s'
        formatter = logging.Formatter(fmt)
        sh.setFormatter(formatter)
        self.logger.addHandler(sh)

    def info(self, func):
        def wrapper(*args, **kwargs):
            self.logger.info(f'Started executing {func.__name__}')
            self.logger.info(f'{func.__name__} got args: {args[1:]}')
            value = func(*args, **kwargs)
            self.logger.info(f'Finished executing {func.__name__}')
            return value

        return wrapper
