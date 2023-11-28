import logging
import time

from commons.loggers.setup_logger import setup_logging

from exec.service.cob import CloseOfBusiness

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import os

    start_time = time.time()
    acct = os.environ.get('ACCOUNT')
    setup_logging(f"cob-{acct}.log")
    logger.info("=====================================================================================================")
    logger.info(f"Started COB Processing for {acct}")
    cob = CloseOfBusiness(acct=acct)
    cob.run_cob()
    logger.info('Finished COB Processing')
    end_time = time.time()
    logger.info(f"Time taken {end_time - start_time}")
    logger.info("=====================================================================================================")
