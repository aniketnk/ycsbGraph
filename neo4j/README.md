## Quick Start

This section describes how to run YCSB on OrientDB running locally. 

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone git://github.com/ceraslabs/ASRL-YCSB.git
    cd YCSB
    mvn clean package

### 2. Run YCSB
    
Now you are ready to run! First, load the data:

    ./bin/ycsb load neo4j -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run neo4j -s -P workloads/workloada

