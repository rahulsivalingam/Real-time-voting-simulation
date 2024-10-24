from datetime import datetime
import random
import time

import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer
import simplejson as json

from main import delivery_report

conf = {
    'bootstrap.servers': 'localhost:9092'
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer(conf)

if __name__ == '__main__':
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    curr = conn.cursor()

    candidates_query = curr.execute("""
    select row_to_json(cols)
    from (
    select * from candidates
    ) cols;
    """)

    candidates = [candidate[0] for candidate in curr.fetchall()]
    print(candidates)

    if len(candidates) ==0:
        raise Exception("No candidates found for voting")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic']) # creating consumer object

    try:
        while True:
            msg = consumer.poll(timeout= 1.0)
            if msg is None:
                continue
            elif msg.error():

                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)

                vote = {**voter, **chosen_candidate, **{
                    'voting_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'vote': 1
                }}

                #vote = voter | chosen_candidate | {
                #    'voting_time': datetime.utcnow().strftime('%Y-%M-%D %H:%M:%S'),
                #    'vote': 1
                #}

                try:
                    print(f"The voter: {vote['voter_id']} voted for candidate: {vote['candidate_id']}")
                    curr.execute("""
                        insert into votes(voter_id, candidate_id, voting_time)
                        values(%s, %s, %s)
                        """, (vote['voter_id'], vote['candidate_id'], vote['voting_time']))
                    conn.commit()

                    producer.produce(
                        'voters_topic',
                        key=vote['voter_id'],
                        value=json.dumps(vote),
                        on_delivery=delivery_report
                    )
                    producer.poll(0)
                except Exception as e:
                    print(e)
            time.sleep(0.5)
    except Exception as e:
        print(e)




