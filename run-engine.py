from commons.loggers.setup_logger import setup_logging
import exec.service.engine as slm
import logging
import time

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import os

    start_time = time.time()
    acct = os.environ.get('ACCOUNT')
    setup_logging(f"engine-{acct}.log")
    logger.info("=====================================================================================================")
    logger.info(f"Started engine Processing for {acct}")
    sl = slm.start(acct)
    logger.info('Finished engine Processing')
    end_time = time.time()
    logger.info(f"Time taken {end_time - start_time}")
    logger.info("=====================================================================================================")
