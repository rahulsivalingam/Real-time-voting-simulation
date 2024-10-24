Scope: 

This project aims to simulate a real time voting system displaying the leading candidate during the election process along with the percentage of votes the other candidates are receiving and the location from where the voters are voting from. 

First we create the required tables for voters, candidates and the voting information. 
Next we create sample candidates and voters from random user api.
Candidates are assigned to a party and then they are populated into the respective tables. 
Push the voters data in a topic. 

Now take this voters data from the topic and create a simulation of the voter voting for a candidate at a particular time and push it back to the topic. Here we do checks to make sure that the voter data when extracted from the topic comes in a json format and then proceed. The candidate data is pulled in from the postgres table via a connection and a query. If all these work out correctly, push this voting information into the voting table and the voting data to “voters_topic” kafka topic. 

Now we need to do the spark streaming which is used to fetch live voting data. We will subscribe to the voters_topic and extract data from it and store it in a DF with a custom required format. From this DF the data will be pushed into 2 separate topics, one to keep count of the votes and the candidate that is leading and the other to keep track of the locations voters are voting in from. I am also adding in a watermark to ensure that if data does not come within a certain window, that does not have to be considered. 

Finally we create a front end using “streamlit” to display the leading candidate, the number of votes they have, the votes the other candidates are getting and the physical location from where the votes are coming in. 

The number of voters and candidates are fetched from the postgres table and presented as the voting statistics. 

Architecture:

The entire project is set up using docker. Kafka brokers, postgres servers are hosted on docker. 


Challenges:

Got error ModuleNotFoundError: No module named 'kafka.vendor.six.moves'\

Got an error between the system python version and python-kafka version needed for live stream. I currently have python version 3.12.6 which does not run well with kafka version 2.0.2. hence had to downgrade the existing system python version from 3.12.6 to 3.11 to proceed with the project. Had to create a new virtual env with its running engine as python 3.11 to continue. 

On MAC default python version is openssl. This was not not supported by the project I was trying to run. Hence had to point the system python to run with urlib rather than openssl.

While reading in data from the kafka stream, had to make sure that the data was coming in with thte correct json format. Hence made a check to ensure if the data was not or not first, if not null then is it in the json format or not. We are decoding the data into utf-8 and then reading it in hence it is important that there is data to decode and the data that comes in after our processing is decoded as needed. 

Very important to notice the spellings of topics when reading or writing from it. 

![image](https://github.com/user-attachments/assets/67a35b9e-0a98-4e9d-85ca-258de1c09bc1)
