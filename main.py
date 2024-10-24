import json

import psycopg2
import requests
import random
from confluent_kafka import SerializingProducer

BASE_URL = 'https://randomuser.me/api?nat=gb'
PARTIES = ['Management_party', 'Saviour_Party', 'Tech_Republic_Party','Bangalore_party','Glasgow_party']
random.seed(21)


###########################################################################
def create_tables(conn, curr):
    curr.execute("""

    create table if not exists candidates(
        candidate_id varchar(255) primary key,
        candidate_name varchar(255),
        party_affiliation varchar(255),
        biography text,
        campaign_platform text,
        photo_url text
        )
    """)

    curr.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    curr.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)
    conn.commit()


##############################################################################
def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))

    if response.status_code == 200:
        user_data = response.json()['results'][0]

        return {
            'candidate_id': user_data['login']['uuid'],
            'candidate_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'party_affiliation': PARTIES[candidate_number % total_parties],
            'biography': 'a brief about the candidate',
            'campaign_platform': 'Key campaign promises and or platform',
            'photo_url': user_data['picture']['large']

            # 'candidate_number' : user_data[][]
        }
    else:
        print("Error fetching data")

def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return{
            'voter_id': user_data['login']['uuid'],
            'voter_name' : f"{user_data['name']['first']} {user_data['name']['last']}",
            'date_of_birth': user_data['dob']['date'],
            'gender': user_data['gender'],
            'nationality': user_data['nat'],
            'registration_number': user_data['login']['username'],
            'address': {
                'street': f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                'city': user_data['location']['city'],
                'state': user_data['location']['state'],
                'country': user_data['location']['country'],
                'postcode': user_data['location']['postcode']
            },
            'email': user_data['email'],
            'phone_number': user_data['phone'],
            'picture': user_data['picture']['large'],
            'registered_age': user_data['registered']['age'],


        }
    else:
        return "Error fetching Data"

def insert_voters(conn, curr, voter):
    #voter = generate_voter_data()
    curr.execute("""
        insert into voters(voter_id, voter_name, date_of_birth, gender, nationality, registration_number,
                           address_street, address_city, address_state, address_country, address_postcode,
                           email, phone_number, picture, registered_age)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """,
    (voter['voter_id'], voter['voter_name'], voter['date_of_birth'], voter['gender'], voter['nationality'],
     voter['registration_number'], voter['address']['street'], voter['address']['city'], voter['address']['state'],
     voter['address']['country'], voter['address']['postcode'], voter['email'], voter['phone_number'],
     voter['picture'], voter['registered_age']))
    conn.commit()

def delivery_report(err, msg):
    if err is not None:
        print(f"The error occurred is {err}")
    else:
        print(f"The message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


##############################################################################
if __name__ == '__main__':

    #producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    producer = SerializingProducer({'bootstrap.servers': 'b-1.realtimevotingkafkams.u8eozn.c4.kafka.eu-north-1.amazonaws.com:9098'})

    try:
        conn = psycopg2.connect("host = localhost dbname = voting user=postgres password=postgres")
        curr = conn.cursor()  # connecting to PG and creating tables and everything else

        create_tables(conn, curr)
        curr.execute("""
            select * from candidates;
        """)

        candidates = curr.fetchall()

        total_parties = len(PARTIES)
        number_of_candidates = 10

        if len(candidates) == 0:
            for i in range(number_of_candidates):
                candidate = generate_candidate_data(i, total_parties)
                curr.execute("""
                    insert into candidates(candidate_id, candidate_name, party_affiliation,biography,campaign_platform,photo_url)
                    values(%s,%s,%s,%s,%s,%s)
                """, (candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'],
                      candidate['biography'], candidate['campaign_platform'], candidate['photo_url'])
                             )
                conn.commit()

        required_number_of_voters = 1000
        for i in range(required_number_of_voters):
            voter_data = generate_voter_data()

            insert_voters(conn, curr, voter_data)

            producer.produce(
                "voters_topic",
                key = voter_data['voter_id'],
                value = json.dumps(voter_data),
                on_delivery=delivery_report # just to make sure data is getting delivered or not

            )

            print(f"produced voter {i}, data:{voter_data}")
            producer.flush()



    except Exception as e:
        print(e)


# Number of voters , number of candidates, number of party