import pika


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='192.168.99.100',
        port=32769,
    ))
    channel = connection.channel()
    channel.queue_declare(queue='test')
    channel.basic_publish(
        exchange='',
        routing_key='test',
        body='Hello World!'
    )
    channel.basic_consume(
        callback,
        queue='test',
        no_ack=True
    )
    channel.start_consuming()

