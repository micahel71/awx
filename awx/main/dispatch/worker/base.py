# Copyright (c) 2018 Ansible by Red Hat
# All Rights Reserved.

import os
import logging
import signal
import sys
from uuid import UUID
from Queue import Empty as QueueEmpty

from kombu import Producer
from kombu.mixins import ConsumerMixin

from awx.main.dispatch.pool import WorkerPool

logger = logging.getLogger('awx.main.dispatch')


def signame(sig):
    return dict(
        (k, v) for v, k in signal.__dict__.items()
        if v.startswith('SIG') and not v.startswith('SIG_')
    )[sig]


class WorkerSignalHandler:

    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)

    def exit_gracefully(self, *args, **kwargs):
        self.kill_now = True


class RoundRobinStrategy(object):
    '''
    Distribute messages based on int(UUID) % total_workers
    This is used for callback event delivery, where we want to dispatch *as
    fast as possible*, but don't care as much about even distribution amongst
    the worker processes (because each event is handle very quickly).
    '''

    def schedule(self, consumer, body, message):
        if "uuid" in body and body['uuid']:
            try:
                queue = UUID(body['uuid']).int % len(consumer.pool)
            except Exception:
                queue = consumer.total_messages % len(consumer.pool)
        else:
            queue = consumer.total_messages % len(consumer.pool)
        return queue


class LeastBusyStrategy(object):
    '''
    Attempt to deliver messages to the *least busy* worker (the worker with the
    shortest work queue).
    This is used for task dispatch, where we want to schedule *evenly* to run
    as many simultaneous tasks as possible (ideally, each worker _never_ has
    a message backlog).
    '''

    def schedule(self, consumer, body, message):
        queue = 0
        size = sys.maxint
        for i, worker in enumerate(consumer.pool.workers):
            if len(worker.managed_tasks) < size:
                queue = i
                size = len(worker.managed_tasks)
        return queue


class AWXConsumer(ConsumerMixin):

    def __init__(self, name, connection, worker, queues=[], pool=None, strategy=RoundRobinStrategy):
        self.connection = connection
        self.total_messages = 0
        self.queues = queues
        self.worker = worker
        self.pool = pool
        self.strategy = strategy()
        if pool is None:
            self.pool = WorkerPool()
        self.pool.init_workers(self.worker.work_loop)

    def get_consumers(self, Consumer, channel):
        logger.debug("Listening on {}".format(self.queues))
        return [Consumer(queues=self.queues, accept=['json'],
                         callbacks=[self.process_task])]

    def control(self, body, message):
        logger.warn(body)
        if body['control'] == 'status':
            producer = Producer(
                channel=self.connection,
                routing_key=message.properties['reply_to']
            )
            producer.publish(self.pool.debug())
        elif body['control'] == 'reload':
            for worker in self.pool.workers:
                worker.quit()
        else:
            logger.error('unrecognized control message: {}'.format(body['control']))
        message.ack()

    def process_task(self, body, message):
        if 'control' in body:
            return self.control(body, message)
        queue = self.strategy.schedule(self, body, message)
        self.pool.write(queue, body)
        self.total_messages += 1
        message.ack()

    def run(self, *args, **kwargs):
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        self.worker.on_start()
        super(AWXConsumer, self).run(*args, **kwargs)

    def stop(self, signum, frame):
        self.should_stop = True  # this makes the kombu mixin stop consuming
        logger.debug('received {}, stopping'.format(signame(signum)))
        self.worker.on_stop()
        raise SystemExit()


class BaseWorker(object):

    def work_loop(self, queue, finished, idx, *args):
        ppid = os.getppid()
        signal_handler = WorkerSignalHandler()
        while not signal_handler.kill_now:
            # if the parent PID changes, this process has been orphaned
            # via e.g., segfault or sigkill, we should exit too
            if os.getppid() != ppid:
                break
            try:
                body = queue.get(block=True, timeout=1)
                if body == 'QUIT':
                    break
            except QueueEmpty:
                continue
            except Exception as e:
                logger.error("Exception on worker, restarting: " + str(e))
                continue
            try:
                self.perform_work(body, *args)
            finally:
                if 'uuid' in body:
                    uuid = body['uuid']
                    logger.debug('task {} is finished'.format(uuid))
                    finished.put(uuid)
        logger.warn('worker exiting gracefully pid:{}'.format(os.getpid()))

    def perform_work(self, body):
        raise NotImplementedError()

    def on_start(self):
        pass

    def on_stop(self):
        pass
