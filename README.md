# pyspark-streaming
Realtime machine learning using pyspark and kafka

Scope of this code

Assume that you are working as an analyst for “MyMoney”, a FinTech company. The app has collected some interesting characteristics of customers who had visited the mall earlier. (Refer the dataset https://www.kaggle.com/ntnu-testimon/paysim1 (Links to an external site.) or you are free to use other datasets also).
Your the team want to catch those bad people, the bad actors before doing transaction have to be informed to the management team.

Part 1:
Assume the customers are doing the transaction randomly. At any time. Develop a kafka topic and create a streaming model to store all these transaction details to the master dataset. Make necessary assumptions about the format of customer transaction details.

Part 2:
Now you have to wear your hat of Security expect! Use the referred dataset and develop a machine learning model to detect the customer is good or bad. Make suitable assumptions while detecting. Briefly explain the strategy used for the same.

Part  3:
Now it’s time to roll out these procedures to the identified customers. For that purpose, you need to process the customer transaction details received through the Kafka topic. You can think of the following activities to be carried out on the stream of customer data within the Apache Spark.
•	Preprocessing on the incoming messages
•	Determining the customer type either good or bad
•	Filtering out the customers based on their transaction type.
•	Join the customer stream with the customer dataset
•	Propose a suitable alert to the transaction approval team based on the enriched data stream
•	Provide this transaction suggestion details to a designated Kafka topic so that downstream application can make use of it to actually send it through SMS.

