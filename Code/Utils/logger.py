import logging

module_loggers = {}

def get_logger(module_name: str) -> logging.Logger :
    if module_name not in module_loggers :
        logger = logging.getLogger(module_name)
        logger.setLevel(logging.DEBUG)
        module_loggers[module_name] = logger
    return module_loggers[module_name]