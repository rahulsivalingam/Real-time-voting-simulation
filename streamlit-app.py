import time
import six
import pandas as pd
import psycopg2
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer
from kafka.consumer import KafkaConsumer
import matplotlib.pyplot as plt
import numpy as np
from streamlit_autorefresh import st_autorefresh

st.title("Realtime Election Dashboard")

@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    curr  = conn.cursor()

    # Getting the total number of voters
    curr.execute("""
    select count(*) as voters_count from voters
    """)
    voters_count = curr.fetchone()[0]

    # Getting the total number of candidates
    curr.execute("""
    select count(*) as candidate_count from candidates
    """)
    candidate_count = curr.fetchone()[0]

    return voters_count, candidate_count




def try_decoding_message(message):
    try:
        return json.loads(message.decode('utf-8'))
    except UnicodeDecodeError as e:
        print(f"Message not encoded {message}, error: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Message not json format: {message},error: {e}")
        return None


def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers = 'localhost:9092',
        auto_offset_reset = 'earliest',
        #value_deserializer = lambda x: json.loads(x.decode('utf-8')) if x else None
        value_deserializer = lambda x: try_decoding_message(x)
    )
    return consumer





def fetch_data_from_kafka(consumer):
    raw_messages = consumer.poll(timeout_ms = 1000)
    data = []
    for to, messages in raw_messages.items():
        for message in messages:
            if message.value is not None:
                try:
                    print(f"raw message received: {message.value}")
                    data.append(message.value)
                except json.JSONDecodeError as e:
                    print(f"failed delivering message: {message.value}, error: {e}")
            else:
                print("Skipping due to empty message")
    return data



def plot_bar_graph(results):
    datatype = results['candidate_name']
    colours = plt.cm.viridis(np.linspace(0,1, len(datatype)))

    fig, ax = plt.subplots()
    bars = ax.bar(datatype, results['total_votes'],color = colours)
    for bar in bars:
        yval = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2, yval, int(yval), ha = 'center', va='bottom')

    plt.xlabel("Candidate")
    plt.ylabel("Total Votes")
    plt.title("Votes per candidate")
    plt.xticks(rotation= 90)
    return plt




def plot_donut_chart(results):
    labels = list(results['candidate_name'])
    sizes = list(results['total_votes'])

    fig, ax = plt.subplots()
    ax.pie(sizes, labels = labels, autopct = '%1.1f%%', startangle=70)
    ax.axis('equal')
    plt.title('candidate votes')

    return fig
def sidebar():
    if st.session_state.get('latest_update') is None:
        st.session_state['last_update'] = time.time()

    refresh_interval = st.sidebar.slider("Refresh interval(s)",5,60,10)
    st_autorefresh(interval = refresh_interval*1000, key = 'auto')

    if st.sidebar.button("Refresh Data"):
        update_data()

@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i: i+rows -1 :] for i in range(0, len(input_df), rows)]
    return df

def pagination_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options = ['Yes','No'], horizontal =1, index = 1)
    if sort =='Yes':
        with top_menu[1]:
            sort_field = st.selectbox("Sort by", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio("Direction", options = ["⬆️", "⬇️"], horizontal=True)
        table_data = table_data.sort_values(by = sort_field, ascending = sort_direction == "⬆️", ignore_index = True)
    pagination = st.container()

    bottom_menu = st.columns((4,1,1))
    with bottom_menu[2]:
        batch_size = st.selectbox("page size", options= [10,25,50,100]
                                  )
    with bottom_menu[1]:
        total_pages = (
            int(len(table_data)/ batch_size) if int(len(table_data)/batch_size) > 0 else 1
        )
        current_page = st.number_input("page", min_value = 1, max_value=total_pages, step=1)

    with bottom_menu[0]:
        st.markdown(f"page **{current_page}** of **{total_pages}** ")

    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page-1], use_container_width = True)



def update_data():
    last_refresh = st.empty()
    #last_refresh.text(f"Last refresh at: {time.strftime("%Y-%m-%d %H:%M:%S")}")
    last_refresh_text = "Last refresh at: {}".format(time.strftime("%Y-%m-%d %H:%M:%S"))
    last_refresh.text(last_refresh_text)


    voters_count, candidate_count = fetch_voting_stats()

    st.markdown("""___""")
    col1, col2 = st.columns(2)
    col1.metric("Total Number of Voters", voters_count)
    col2.metric("total number of candidates", candidate_count)

    consumer = create_kafka_consumer(topic_name)
    data = fetch_data_from_kafka(consumer)
    #results = pd.DataFrame(data)
    #return results


    if data:

        results = pd.DataFrame(data)

        # see who is winning at the moment

        results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
        winning_candidate = results.loc[results['total_votes'].idxmax()]

        st.markdown("""---""")
        st.header("Leading Candidate")
        col1, col2 = st.columns(2)
        with col1:
            st.image(winning_candidate['photo_url'],width = 250)
        with col2:
            st.header(winning_candidate['candidate_name'])
            st.subheader(winning_candidate['party_affiliation'])
            st.subheader(winning_candidate['total_votes'])


        st.markdown("""---""")
        st.header("Voting Stats")
        results = results[['candidate_id','candidate_name','party_affiliation','total_votes']]
        results=results.reset_index(drop = True)


        col1, col2 = st.columns(2)

        with col1:
            bar_graph = plot_bar_graph(results)
            st.pyplot(bar_graph)

        with col2:
            donut_chart = plot_donut_chart(results)
            st.pyplot(donut_chart)

        st.table(results)


        location_consumer = create_kafka_consumer('aggregated_turnout_location')
        location_data = fetch_data_from_kafka(location_consumer)
        location_result = pd.DataFrame(location_data)

        location_result = location_result.loc[location_result.groupby('state')['count'].idxmax()]
        location_result = location_result.reset_index(drop=True)

        st.header("Voters Location")
        pagination_table(location_result)



    else:
        st.write("No Data in Topic")

topic_name = "aggregated_votes_per_candidate"
sidebar()

update_data()

