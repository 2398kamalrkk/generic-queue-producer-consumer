from queue import Queue
from multipledispatch import dispatch
import localstack_client.session

session = localstack_client.session.Session()
sqs = session.client('sqs')

#Wrapper class for message
q = Queue(maxsize=10)
class Wrapper:
    def __init__(self,message,hits):
        self.message = message
        self.hits = hits + 1
    def get_message(self):
        return self.message
    def get_hits(self):
        return self.hits


class Factory:
    def enQueue(self,wrapper,queue_name):
        sqs.create_queue(
            QueueName=queue_name)
        url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
        sqs.send_message(
            QueueUrl=url,
            MessageBody='string',
            DelaySeconds=20,
            MessageAttributes={
                'message': {
                    'StringValue': wrapper.get_message(),
                    'DataType': 'String'
                },
                'hits': {
                    'StringValue': str(wrapper.get_hits()),
                    'DataType': 'String'
                }
            },

            MessageDeduplicationId='string',
            MessageGroupId='string'
        )
    def deQueue(self,queue_name):
        url = sqs.get_queue_url(QueueName=queue_name)['QueueUrl']
        queue_received_object = sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=1,
            VisibilityTimeout=1,
            WaitTimeSeconds=2,
        )["Messages"][0]
        sqs.delete_message(
            QueueUrl=url,
            ReceiptHandle=queue_received_object["ReceiptHandle"]
        )
        return queue_received_object


@dispatch(str,str)
def produce(message,queue_name):
    wrapper_object = Wrapper(message, 0)
    fac = Factory()
    fac.enQueue(wrapper_object,queue_name)

@dispatch(Wrapper,str)
def produce(wrapper,queue_name):
    wrapper_object = Wrapper(wrapper.get_message(), wrapper.get_hits())
    fac = Factory()
    fac.enQueue(wrapper_object, queue_name)

def consume(queue_name):
    fac = Factory()
    return fac.deQueue(queue_name)



produce("MESSAGE NEW FROM 1","JJJJ")
produce(consume("JJJJ"),"JJJJ")
print(consume("test"))
