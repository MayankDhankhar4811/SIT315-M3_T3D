#include <iostream> // Standard input-output stream
#include <fstream> // Input-output file stream
#include <vector> // Standard vector container
#include <algorithm> // Standard algorithms library
#include <pthread.h> // POSIX Threads library for multi-threading

#include <mpi.h> // Message Passing Interface library for parallel computing
#include <iomanip> // Input-output manipulation library
#include <string> // Standard string library
#include <sstream> // String stream processing
#include <iterator> // Iterator library
#include <time.h> // Date and time functions
#include <stdlib.h> // Standard library functions

// Function to initialize MPI environment
void InitializeMPI() {
    MPI_Init(NULL, NULL); 
}

// Function to get the number of processes
int GetProcessesCount() {
    int process_num;
    MPI_Comm_size(MPI_COMM_WORLD, &process_num); 
    return process_num;
}

// Function to get the process ID
int GetProcessId() {    
    int process_Id;
    MPI_Comm_rank(MPI_COMM_WORLD, &process_Id); 
    return process_Id;
}

// Function to get the name of the processor
std::string GetProcessName() {
    char processorName[MPI_MAX_PROCESSOR_NAME];
    int nameLen;
    MPI_Get_processor_name(processorName, &nameLen); 
    return std::string(processorName);
}

// Function to broadcast data to all processes
void MPIBroadcast(void *input, int inputSize, MPI_Datatype type, int root_process_id, MPI_Comm comm){
    MPI_Bcast(input, inputSize, type, root_process_id, comm); 
}

// Function to send data to a particular process
void MPISend(void *input, int inputSize, MPI_Datatype type, int dest_processId, MPI_Comm comm){
    MPI_Send(input, inputSize, type, dest_processId, 0, comm); 
}

// Function to receive data from a particular process
void MPIRecieve(void *buffer, int incomingDataCount, MPI_Datatype type, int sender_process_id, MPI_Comm comm){
    MPI_Recv(buffer, incomingDataCount, type, sender_process_id, 0, comm, MPI_STATUS_IGNORE); 
}

// Function to finalize the MPI environment
void FinalizeMPI(){
    MPI_Finalize(); 
}

using namespace std;

int PROD_TH; // Producer thread count
int CONS_TH; // Consumer thread count
int QUEUE_SIZE; // Queue size for traffic signals
 
struct TrafficSignal {
    int trafficId; // Unique ID of the traffic signal
    int carsPassed; // Number of cars passed through the signal
    int timestamp; // Timestamp of the signal
};

// Structure to pass thread arguments
struct ThreadArgs {
    int threadId; // Thread ID
    int lineId; // Line ID
};

// Global variables
vector<TrafficSignal> signals; // Vector to store traffic signals
vector<TrafficSignal> signalQueue; // Queue to hold traffic signals
vector<TrafficSignal> results; // Vector to store results
int CURRENT_ROW = 0; // Current row index

// Thread variables
vector<pthread_t> producerThreads; // Vector to hold producer threads
vector<pthread_t> consumerThreads; // Vector to hold consumer threads
pthread_mutex_t accessLock; // Mutex lock for thread synchronization
MPI_Datatype MPI_DATASIGNAL; // MPI custom data type for TrafficSignal

// Function to split a string into substrings based on a delimiter
vector<string> Split_string(string prompt, string delimiter){
    vector<string> list; // Vector to store substrings
    string in_string = string(prompt); // Input string
    size_t pos = 0; // Position of delimiter
    string ex_string; // Extracted substring
    
    while ((pos = in_string.find(delimiter)) != string::npos) {
        ex_string = in_string.substr(0, pos); // Extract substring
        list.push_back(ex_string); // Add substring to vector
        in_string.erase(0, pos + delimiter.length()); // Remove substring and delimiter
    }
    list.push_back(in_string); // Add the last substring
    return list; // Return vector of substrings
}

// Function to generate traffic data and save it to a file
void generate_traffic() {
    srand(time(NULL)); // Seed random number generator
    ofstream myfile;
    myfile.open("traffic_data_mpi.txt"); // Open file for writing

    // Set starting timestamp and max number of traffic signals
    long currentTimestamp = time(NULL);
    const int MAX_ID = 12;

    // Generate traffic signals and write to file
    for (int i = 0; i < 6000; i++) {
        currentTimestamp += 5; // Increment timestamp
        int id = rand() % MAX_ID; // Generate random ID
        string signal = to_string(id) + "," + to_string(rand() % 5) + "," + to_string(currentTimestamp); // Construct signal string
        myfile << signal + "\n"; // Write signal to file
    }

    myfile.close(); // Close file
}

// Function to load traffic data from file into memory
void LoadData() {
    ifstream myfile;
    myfile.open("traffic_data_mpi.txt"); // Open file for reading
    string line;

    // Read each line and parse traffic signal data
    while (getline(myfile, line)) {
        vector<string> spilts = Split_string(line, ","); // Split line into substrings
            
        TrafficSignal sig = TrafficSignal();
        sig.trafficId = atoi(spilts[0].c_str()); // Convert ID to integer
        sig.carsPassed = atoi(spilts[1].c_str()); // Convert cars passed to integer
        sig.timestamp = atoi(spilts[2].c_str()); // Convert timestamp to integer

        signals.push_back(sig); // Add signal to vector
    }

    reverse(signals.begin(), signals.end()); // Reverse array
    CURRENT_ROW = results.size() - 1; // Update current row index

    cout<<"[MAIN] "<<to_string(signals.size()) << " signals has been loaded."<<endl; // Output number of signals loaded
}

// Function to compare traffic signals based on number of cars passed
bool compareSignals(TrafficSignal s1, TrafficSignal s2) { 
    return (s1.carsPassed > s2.carsPassed); 
}

// Producer thread function
void* Producer(void *input) {
    ThreadArgs* args = (struct ThreadArgs*) input; // Cast void pointer to ThreadArgs pointer

    while (true) {
        if (signals.size() == 0) { // Exit thread if no more signals to process
            pthread_exit(NULL);
        }
        pthread_mutex_lock(&accessLock); // Lock mutex
        
        if (!signals.empty() && signalQueue.size() < QUEUE_SIZE) { // Validate for recursive purposes
            TrafficSignal &sig = signals[signals.size() - 1]; // Get last signal from vector
            signals.pop_back(); // Remove signal from vector
            signalQueue.push_back(sig); // Add signal to queue
            args->lineId = CURRENT_ROW++; // Update line ID
        }
        pthread_mutex_unlock(&accessLock); // Unlock mutex
    }
}

// Consumer thread function
void* Consumer(void *input) {
    ThreadArgs* args = (struct ThreadArgs*) input; // Cast void pointer to ThreadArgs pointer

    while (true) {
        if (signals.size() == 0 && signalQueue.size() == 0){ // Exit thread if zero signals and queue is empty
            pthread_exit(NULL);
        }
        pthread_mutex_lock(&accessLock); // Lock mutex

        if (signalQueue.size() > 0){
            TrafficSignal sig = signalQueue[0]; // Get first signal from queue
            signalQueue.erase(signalQueue.begin()); // Remove signal from queue
            bool exists = false;

            // Find the corresponding traffic id from array and update it
            for (int i = 0; i < results.size(); i++) {
                if (sig.trafficId == results[i].trafficId){
                    results[i].carsPassed += sig.carsPassed;
                    exists = true;
                }
            }
            
            // Add traffic signal to array if it does not exist
            if (exists == false) {
                results.push_back(sig);
            }

            sort(results.begin(), results.end(), compareSignals); // Sort array
        }
        pthread_mutex_unlock(&accessLock); // Unlock mutex
    }
}

// Function to create producer threads
void Create_producer() {
    for (int i = 0; i < PROD_TH; i++) {
        pthread_t t;
        producerThreads.push_back(t); // Initialize thread array
    }

    // Start producer threads
    for (int i = 0; i < PROD_TH; i++) {
        ThreadArgs *args = (struct ThreadArgs *) malloc(sizeof(struct ThreadArgs)); // Allocate memory for thread arguments
        args->threadId = i;
        args->lineId = CURRENT_ROW++;

        pthread_create(&producerThreads[i], NULL, Producer, (void *) args); 
    }
}

// Function to create consumer threads
void Create_consumer() {
    for (int i = 0; i < CONS_TH; i++) {
        pthread_t t;
        consumerThreads.push_back(t); // Initialize thread array
    }

    // Start consumer threads
    for (int i = 0; i < CONS_TH; i++) {
        ThreadArgs *args = (struct ThreadArgs *) malloc(sizeof(struct ThreadArgs)); // Allocate memory for thread arguments
        args->threadId = i;
        args->lineId = 0;

        pthread_create(&consumerThreads[i], NULL, Consumer, (void *) args); 
    }
}

// Function to coordinate producer and consumer threads
void resolve() {
    if (pthread_mutex_init(&accessLock, NULL) != 0) { // Initialize the access lock mutex
        printf("\n[MAIN]: Access Mutex lock init has failed.\n"); 
        return; 
    } 

    Create_consumer(); // Create consumer threads
    Create_producer(); // Create producer threads
    
    // Wait for producer threads to finish
    for (int i = 0; i < PROD_TH; i++){
        pthread_join(producerThreads[i], NULL);
    }

    // Wait for consumer threads to finish
    for (int i = 0; i < CONS_TH; i++){
        pthread_join(consumerThreads[i], NULL);
    }

    pthread_mutex_destroy(&accessLock); // Destroy the access lock mutex
}

// Function to calculate the chunk size based on the number of hours
int GetChunkSize(int hours) {
    return (12 * hours); // Return the chunk size
}

// Function to print a table row
void printTableRow(const std::string& col1, const std::string& col2) {
    const int colWidth = 18;
    std::cout << "| " << std::left << std::setw(colWidth) << col1;
    std::cout << "| " << std::left << std::setw(colWidth) << col2;
    std::cout << "|" << std::endl;
}

// Function to print a decorative line
void printDecorativeLine(int length, char symbol) {
    std::cout << std::string(length, symbol) << std::endl;
}

// Function to execute the main functionality
void Head() {
    generate_traffic(); // Generate traffic data
    LoadData(); // Load file into memory

    int process_num = GetProcessesCount(); // Get the number of processes
    const int chunkSize = GetChunkSize(24); // Calculate the chunk size based on the total number of signals and the number of processes
    const int chunksCount = signals.size() / chunkSize; // Calculate the total number of chunks based on the signal size and chunk size
    int chunkId = 0; // Initialize the chunk ID
    int complete = 0; // Initialize the completion status

    // Send chunks to other processes
    while (chunkId < chunksCount) {
        for (int pid = 0; pid < process_num; pid++) {
            if (pid == 0) continue; // Skip the first process (0)

            int chunkStart = (chunkId * chunkSize); // Calculate the start index of the current chunk
            int chunkEnd = chunkStart + chunkSize > signals.size() ? signals.size() : chunkStart + chunkSize; // Calculate the end index of the current chunk

            MPIBroadcast(&complete, 1, MPI_INT, 0, MPI_COMM_WORLD); // Send the current completion status to all processes

            vector<TrafficSignal> signalsChunk(signals.begin() + chunkStart, signals.begin() + chunkEnd); // Create a chunk of signals from the main signal vector

            int size = signalsChunk.size();
            MPISend(&size, 1, MPI_INT, pid, MPI_COMM_WORLD); // Send the size of the chunk of signals to the target process

            MPISend(&chunkId, 1, MPI_INT, pid, MPI_COMM_WORLD); // Send the ID of the current chunk to the target process

            MPISend(&signalsChunk[0], size, MPI_DATASIGNAL, pid, MPI_COMM_WORLD); // Send the signal data to the target process

            chunkId += 1; // Increment the chunk ID for the next iteration
        }
    }

    // Wait for completion of processes
    while (true) {
        for (int pid = 0; pid < process_num; pid++) {
            if (pid == 0) continue; 

            int size;
            MPIRecieve(&size, 1, MPI_INT, pid, MPI_COMM_WORLD); // Receive size of chunk of signals

            int subChunkId;
            MPIRecieve(&subChunkId, 1, MPI_INT, pid, MPI_COMM_WORLD); // Receive id of current chunk

            results.clear();
            results.resize(size);
            MPIRecieve(&results[0], size, MPI_DATASIGNAL, pid, MPI_COMM_WORLD); // Receive signal data
            chunkId += 1;

            // Print top congested lights
            std::cout << "                DAY " << chunkId << "                      " << std::endl;
            std::cout << "+-----------------+-----------------+" << std::endl;
            printTableRow("Traffic ID", "Number of Cars");
            std::cout << "+-----------------+-----------------+" << std::endl;

            // Table rows for congested signals
            for (int i = 0; i < numCongestedSignals && i < results.size(); i++) {
                std::string trafficId = "ID: " + std::to_string(results[i].trafficId);
                std::string carsPassed = std::to_string(results[i].carsPassed);
                printTableRow(trafficId, carsPassed);
            }

            std::cout << "+-----------------+-----------------+" << std::endl;
            std::cout << std::endl;
        }
        if (chunkId >= chunksCount) break; // Exit loop if all chunks processed
    }
}

// Function for slave processes to execute
void Slave() {
    int process_Id = GetProcessId();
    int complete;
    int globalChunkId = 0;

    // Receive completion status
    while (true) {
        MPIBroadcast(&complete, 1, MPI_INT, 0, MPI_COMM_WORLD);
        if (complete == 1) {
            printf("Process (%d) is done...", process_Id);
            break;
        }

        // Receive size of signals
        int size;
        MPIRecieve(&size, 1, MPI_INT, 0, MPI_COMM_WORLD);

        int chunkId;
        MPIRecieve(&chunkId, 1, MPI_INT, 0, MPI_COMM_WORLD);

        // Buffer signals into array
        signals.clear();
        signals.resize(size);
        results.clear();
        results.resize(0);

        MPIRecieve(&signals[0], size, MPI_DATASIGNAL, 0, MPI_COMM_WORLD);

        resolve(); // Resolve congestion

        sort(results.begin(), results.end(), compareSignals); // Sort results

        // Send size of results array
        size = results.size();
        MPISend(&size, 1, MPI_INT, 0, MPI_COMM_WORLD);

        // Send chunk id
        MPISend(&chunkId, 1, MPI_INT, 0, MPI_COMM_WORLD);

        // Send results array
        MPISend(&results[0], size, MPI_DATASIGNAL, 0, MPI_COMM_WORLD);
    }
}

// Main function
int main() {
    srand(time(NULL)); // Seed random number generator

    // Initialize MPI
    InitializeMPI();

    int process_Id = GetProcessId();

    // Create custom MPI data type
    const int nitems = 3;
    int blocklengths[nitems] = {1, 1, 1};
    MPI_Datatype types[nitems] = {MPI_INT, MPI_INT, MPI_INT};
    MPI_Aint items[nitems];

    items[0] = offsetof(TrafficSignal, trafficId);
    items[1] = offsetof(TrafficSignal, carsPassed);
    items[2] = offsetof(TrafficSignal, timestamp);

    MPI_Type_create_struct(nitems, blocklengths, items, types, &MPI_DATASIGNAL);
    MPI_Type_commit(&MPI_DATASIGNAL);

    if (process_Id == 0) {
        Head(); // Execute main functionality
    }
    else {
        Slave(); // Execute slave functionality
    }

    // Print simulator parameters
    if (process_Id == 0) {
        std::cout << std::endl;
        printDecorativeLine(54, '-');
        std::cout << "|         Simulator Parameters (Parallel)         |" << std::endl;
        printDecorativeLine(54, '-');
        std::cout << "| Producer Thread Count: " << std::setw(5) << PROD_TH << "              |" << std::endl;
        std::cout << "| Consumer Thread Count: " << std::setw(5) << CONS_TH << "              |" << std::endl;
        std::cout << "| Queue Size Count    : " << std::setw(5) << QUEUE_SIZE << "               |" << std::endl;
        printDecorativeLine(54, '-');
        std::cout << std::endl;
    }

    MPI_Type_free(&MPI_DATASIGNAL); // Free MPI data type
    FinalizeMPI(); // Finalize MPI environment

    return 1; // Return from main function
}
