# Bus Trajectory Retrieval

This package allows for the retrieval of data from public transportation
real time data. In particular, it is created in order to retrieve and
store **vehicle position** and **trip updates** data every minute.

The retrieval is done by downloading the protobuf files that are provided
by the API endpoint in binary format, according to GTFS standards.

The data is then normalized and stored in a PostgreSQL database.

# Impressum

Shoichi Yip, 2023
