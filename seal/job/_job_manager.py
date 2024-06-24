import schedule
from schedule import Job
import time
from loguru import logger
import traceback


class JobManager:

    def __init__(self):
        self._jobs = {}
        self.started = False

    def register(self, name: str, job: Job, func, success_func=None, error_func=None):
        if self.started:
            raise Exception('job manager already started')

        def func_wrapper():
            try:
                success = func()
                if not success:
                    logger.error(f'job {name} failed')
                    if error_func:
                        try:
                            error_func()
                        except Exception as e:
                            logger.error(f'job {name} error_func error: {e}')
                            logger.error(traceback.format_exc())
                else:
                    logger.info(f'job {name} success')
                    if success_func:
                        try:
                            success_func()
                        except Exception as e:
                            logger.error(f'job {name} success_func error: {e}')
                            logger.error(traceback.format_exc())
            except Exception as e:
                logger.error(f'job {name} error: {e}')
                logger.error(traceback.format_exc())

        self._jobs[name] = {}
        self._jobs[name]['job'] = job
        self._jobs[name]['func'] = func_wrapper

        job.do(func_wrapper)

    def start(self):
        self.started = True
        while True:
            schedule.run_pending()
            time.sleep(1)
