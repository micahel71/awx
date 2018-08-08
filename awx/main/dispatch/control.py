import logging
import socket

from django.conf import settings

from kombu import Connection, Queue, Exchange, Producer, Consumer

logger = logging.getLogger('awx.main.dispatch')


class Control(object):

    services = ('dispatcher', 'callback_receiver')

    def __init__(self, service):
        if service not in self.services:
            raise RuntimeError('{} must be in {}'.format(service, self.services))
        self.service = service
        self.queue = Queue(
            '{}-rpc'.format(service),
            Exchange('{}-rpc'.format(service), type='direct'),
            routing_key='request-{}'.format(settings.CLUSTER_HOST_ID)
        )

    def publish(self, msg, conn, host, **kwargs):
        producer = Producer(
            exchange=self.queue.exchange,
            channel=conn,
            routing_key='request-{}'.format(host)
        )
        producer.publish(msg, expiration=5, **kwargs)

    def status(self, host=None, timeout=5):
        host = host or settings.CLUSTER_HOST_ID
        logger.warn('checking {} status for {}'.format(self.service, host))
        reply_queue = Queue(name="amq.rabbitmq.reply-to")
        with Connection(settings.BROKER_URL) as conn:
            with Consumer(conn, reply_queue, callbacks=[self.process_message], no_ack=True):
                self.publish({'control': 'status'}, conn, host, reply_to='amq.rabbitmq.reply-to')
                try:
                    conn.drain_events(timeout=timeout)
                except socket.timeout:
                    logger.error('{} did not reply within {}s'.format(self.service, timeout))
                    raise

    def control(self, msg, host=None, **kwargs):
        host = host or settings.CLUSTER_HOST_ID
        with Connection(settings.BROKER_URL) as conn:
            self.publish(msg, conn, host)

    def process_message(self, body, message):
        print body
        message.ack()


