from predict.loggers.setup_logger import setup_logging
import predict.service.socketSLmonitor as slm
import logging
import time

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import os

    start_time = time.time()
    acct = os.environ.get('ACCOUNT')
    setup_logging(f"socket-{acct}.log")
    logger.info("=====================================================================================================")
    logger.info(f"Started Socket Processing for {acct}")
    sl = slm.start(acct)
    logger.info('Finished Socket Processing')
    end_time = time.time()
    logger.info(f"Time taken {end_time - start_time}")
    logger.info("=====================================================================================================")
