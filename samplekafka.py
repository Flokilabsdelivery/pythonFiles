from kafka import KafkaProducer
import json
 
try:
    producer = KafkaProducer(bootstrap_servers='MR402S0352D.palawangroup.com:9092')
    topic = 'ftpKafkaConsumer'
 
    for i in range(1):
        message = f"Message {i}"
        my_dict = {'fileUploadId': 314, 'filePath': '/STFS0029M/migration/AUG/2023-08-07', 'fileName': 'CDMS output.csv'}
        my_dict = json.dumps(my_dict)
        producer.send(topic, value=my_dict.encode('utf-8'))
        print("Message sent successfully")
 
except Exception as e:
    print(f"Error: {e}")
finally:
    producer.close()
