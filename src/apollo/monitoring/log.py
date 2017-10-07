import logging
import os

def config_log(log_checkpoint=True, log_debug=False):
    if not os.path.exists('logging'):
        os.makedirs('logging')
    if log_debug:
        logging.basicConfig(filename='logging/debug.log',level=logging.DEBUG)
    
    if log_checkpoint:
        logger = logging.getLogger('holding')
        logger.setLevel(logging.INFO)
        # create file handler which logs even debug messages
        fh = logging.FileHandler('logging/holding.log')
        fh.setLevel(logging.INFO)
        # add the handlers to the logger
        logger.addHandler(fh)

        logger = logging.getLogger('release')
        logger.setLevel(logging.INFO)
        # create file handler which logs even debug messages
        fh = logging.FileHandler('logging/release.log')
        fh.setLevel(logging.INFO)
        # add the handlers to the logger
        logger.addHandler(fh)

    logger = logging.getLogger('general')
    logger.setLevel(logging.INFO)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('logging/general.log')
    fh.setLevel(logging.INFO)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(logging.StreamHandler())

    logger = logging.getLogger('exception')
    logger.setLevel(logging.DEBUG)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('logging/exception.log')
    fh.setLevel(logging.INFO)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(logging.StreamHandler())

    logger = logging.getLogger('commit')
    logger.setLevel(logging.INFO)
    # create file handler which logs even debug messages
    fh = logging.FileHandler('logging/commit.log')
    fh.setLevel(logging.INFO)
    # add the handlers to the logger
    logger.addHandler(fh)