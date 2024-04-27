# Krypton DB

Welcome to Krypton DB, a high-performance LSM Tree database with integrated Bloom Filter support and versatile TCP/UDP request handling.

## Features

- **LSM Tree Architecture:** Krypton DB employs an LSM (Log-Structured Merge) Tree architecture for optimized read and write operations, ensuring exceptional performance even under heavy workloads.

- **Bloom Filter Integration:** With built-in Bloom Filter functionality, Krypton DB enhances query efficiency by reducing disk accesses, resulting in faster data retrieval and improved overall system performance.

- **TCP and UDP Support:** Krypton DB offers seamless support for both TCP and UDP protocols, allowing for flexible and efficient communication with client applications.

- **Write-Ahead Log (WAL):** Krypton DB includes a reliable Write-Ahead Log (WAL) feature, ensuring durability and consistency of data by logging changes before they are applied to the main database.
## Getting Started

To get started with Krypton DB, follow these simple steps:

1. **Clone the Repository:**
   ```bash
   git clone https://github.com/krypton-db/krypton-db.git
   ```
2. **Setup the config file**
   Here is the config file
   ```yaml
   port: 
   host: 
   udpport: 
   udpbuffersize: 
   num_Of_Partitions: 
   directory: 
   maximum_element: 
   compaction_frequency: 
   bloom_capacity: 
   bloom_error_rate: 
   walpath: 
   ```
3. **Run the db**
   ```bash
    go run main.go
   ```
4. **Go to the terminal and open telnet**
   ```bash
    telnet localhost port (for tcp, here port is what you will be defining in yaml)
    eg telnet localhost 8080
    same for udp connection, you need to provide the udp port but it should used for GET Request only
   ```
5. Querying
   
   **PUT Request**
   ```bash
   PUT KEY VALUE 
   ```
   **GET Request**
   ```bash
   GET KEY  
   ```
   **DEL Request**
   ```bash
   DEL KEY
   ```
   
