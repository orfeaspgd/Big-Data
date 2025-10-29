# Traffic Pipeline

Traffic Pipeline is a hands-on project demonstrating the end-to-end management, real-time processing, and storage of vehicle traffic data using open-source technologies. The pipeline integrates a traffic simulator (UXSIM), Kafka for data streaming, Apache Spark for real-time analytics, and MongoDB for NoSQL storage.

## Features
- **Data Generation:** Simulate vehicle movement using UXSIM and export data as pandas DataFrames.
- **Streaming:** Send vehicle position data to a Kafka broker at regular intervals.
- **Real-Time Processing:** Consume and process data streams with Apache Spark, generating analytics such as vehicle counts and average speeds per road segment.
- **NoSQL Storage:** Store both raw and processed data in MongoDB collections for further analysis.
- **Querying:** Run queries on MongoDB to answer traffic-related questions (e.g., least crowded segment, highest average speed, longest route).

## Technologies Used
- Python
- UXSIM (Traffic Simulator)
- Kafka & kafka-python
- Apache Spark & PySpark
- MongoDB & MongoDB Spark Connector
