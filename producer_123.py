from kafka import KafkaProducer
import json
 
try:
    producer = KafkaProducer(bootstrap_servers='10.10.14.82:9092')
    topic = 'ftpKafkaConsumer'
 
    for i in range(10):
        message = f"Message {i}"
        my_dict = {'fileUploadId': 2, 'filePath': '/mnt/', 'fileName': 'CDMS_value.csv'}
        my_dict = json.dumps(my_dict)
        producer.send(topic, value=my_dict.encode('utf-8'))
        print("Message sent successfully")
 
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()




