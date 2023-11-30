import logging
import time

from commons.loggers.setup_logger import setup_logging

from exec.service.cob import CloseOfBusiness

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    import os

    start_time = time.time()
    acct = os.environ.get('ACCOUNT')
    setup_logging(f"cob.log")
    logger.info("=====================================================================================================")
    logger.info(f"Started COB Processing")
    accounts = 'Trader-V2-Alan,Trader-V2-Pralhad,Trader-V2-Sundar,Trader-V2-Mahi'
    cob = CloseOfBusiness()
    cob.run_cob(accounts=accounts)
    logger.info('Finished COB Processing')
    end_time = time.time()
    logger.info(f"Time taken {end_time - start_time}")
    logger.info("=====================================================================================================")
