#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <vector>
#include <utility>
#include <map>
#include <unordered_map>
#include <string>
#include <cstring>
#include <fstream>
#include <set>
#include <iostream>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100
#define UPLOAD_TAG_1 6
#define UPLOAD_TAG 5
#define HASH_REQUEST 10
#define HASH_RECEIVE 11
#define FINISHED_BITTORRENT 12
#define FINISHED_DOWNLOADING_ONE_FILE 13
#define FINISHED_DOWNLOADING_ALL_FILES 14
#define MAXIMUM_FILES 100

using namespace std;

enum REQUEST_TYPE {
    FIRST_CONTACT,
    UPDATE,
    FINISH_DOWNLOADING_ONE_FILE,
    FINISH_DOWNLOADING_ALL_FILES,
    ALL_CLIENTS_DONE
};

enum CLIENT_STATE {
    SEED,
    PEER,
    LEECHER
};

enum CLIENT_TERMINATION_STATE {
    TERMINATED,
    NOT_TERMINATED,
};

typedef struct {
    unsigned int number_of_segments;
    char *fileName;
    char **hashes;
} InitializationRequest;

typedef struct {
    pthread_mutex_t *upload_mutex;
    int rank;
    unsigned int number_of_tasks;
    InitializationRequest **initializationRequest;
    InitializationRequest **saved_files;
    char ***wish_list;
    unsigned int *number_of_files;
    unsigned int *number_of_wishes;
    int ***verified_hashes;
    int *finished_job;
} Arguments;


typedef struct {
    unsigned int client_id;
    unsigned int number_of_files;
    InitializationRequest *fileMetadata;
} TrackerDatabaseEntry;

typedef struct {
    unsigned int client_id;
    CLIENT_STATE client_state;
} ClientInfo;

typedef struct {
    REQUEST_TYPE request_type;
    unsigned int client_id;
    char *desired_file;
    int send_hashes;
} BitTorrentRequest;

InitializationRequest *read_file_retrieve_info(int rank, unsigned int *number_of_files, char ***wish_list, unsigned int *wishes) {
    InitializationRequest *resulted_files;

    std::string fileName = "in" + std::to_string(rank) + ".txt";
    std::ifstream inputFile(fileName.c_str());

    int number_of_possessing_files = 0;
    inputFile >> number_of_possessing_files;
    *number_of_files = number_of_possessing_files;
    resulted_files = (InitializationRequest *) malloc(sizeof(InitializationRequest) * number_of_possessing_files);

    for (int i = 0; i < number_of_possessing_files; i++) {
        std::string currentFilename;
        unsigned int number_of_segments;

        inputFile >> currentFilename >> number_of_segments;
        resulted_files[i].number_of_segments = number_of_segments;
        resulted_files[i].fileName = (char *) malloc(sizeof(char) * MAX_FILENAME);
        strcpy(resulted_files[i].fileName, currentFilename.c_str());
    
        resulted_files[i].hashes = (char**) malloc(sizeof(char*) * number_of_segments);
        unsigned int current_index = 0;
        while (number_of_segments) {
            string current_hash;
            inputFile >> current_hash;

            resulted_files[i].hashes[current_index] = (char *) malloc(sizeof(char) * HASH_SIZE);
            strcpy(resulted_files[i].hashes[current_index], current_hash.c_str());
            current_index++;

            number_of_segments--;
        }
    }

    int number_of_desired_files = 0;
    inputFile >> number_of_desired_files;

    *wishes = number_of_desired_files;

    (*wish_list) = (char **)malloc(sizeof(char *) * number_of_desired_files);
    for (int i = 0; i < number_of_desired_files; i++) {
        char *current_desire = (char *) malloc(sizeof(*current_desire) * MAX_FILENAME);
        inputFile >> current_desire;
        (*(*wish_list + i)) = (char *) malloc(sizeof(char) * MAX_FILENAME);

        strcpy((*wish_list)[i], current_desire);
    }

    return resulted_files;
}

void print_hashes(InitializationRequest *initializationRequest, unsigned int number_of_files) {
    for (unsigned int i = 0; i < number_of_files; i++) {
        InitializationRequest curr = initializationRequest[i];

        for (unsigned int j = 0; j < curr.number_of_segments; j++) {
            cout << curr.hashes[j] << endl;
        }
    }
}

void printRequests(InitializationRequest *requests, int number_of_files, int **verified_hashes) {
    cout << "The number of files is: " << number_of_files << endl;
    for (int i = 0; i < number_of_files; i++) {
        cout << "File name is: " << requests[i].fileName << endl;
        cout << "Number of segments is: " << requests[i].number_of_segments << endl;
        
        cout << "Hashes are: " << endl;
        for (unsigned int j = 0; j < requests[i].number_of_segments; j++) {
            cout << requests[i].hashes[j] << " ";
            if (verified_hashes != NULL) {
                cout << verified_hashes[i][j] << endl;
            }
        }
        cout << endl << endl << endl << endl << endl << endl;
    }
}

void printSwarms(int **swarm, int number_of_members, int number_of_wishes, char **wish_list) {

    cout << "THE SWARMS ARE: " << endl;

    for (int j = 0; j < number_of_wishes; j++) {
        cout << wish_list[j] << "  ------->  "; 
        for (int i = 0; i < number_of_members; i++) {
            cout << swarm[j][i] << " ";
        }

        cout << endl;
    }
}

void *download_thread_func(void *arg)
{
    Arguments argument = *(Arguments *) arg;
    int rank = argument.rank;
    int number_of_tasks = argument.number_of_tasks;

    int number_of_collected_segments = 0;
    unsigned int number_of_files = *argument.number_of_files, number_of_wishes = *argument.number_of_wishes;
    int wished_files_received = 0;

    InitializationRequest *initializationRequest = *argument.initializationRequest;
    InitializationRequest *saved_files = *argument.saved_files;
    char **wish_list = *argument.wish_list;
 
    int **verified_hashes = *argument.verified_hashes;

    for (unsigned int i = 0; i < number_of_wishes; i++) {
        verified_hashes[i] = NULL;
    }
    
    // structura care asigura descarcarea tuturor fisierelor de la alti clienti
    std::map<string, int> downloaded_files_situation;

    // structura pentru a asigura obtinerea hashurilor de la tracker
    std::map<string, int> wished_files_situation;
  
    // initialize receiving files statement. For false, the peer wants the hashes too.
    for (unsigned int i = 0; i < number_of_wishes; i++) {
        wished_files_situation[wish_list[i]] = 0;
        downloaded_files_situation[wish_list[i]] = 0;
    }

    // trimite numarul de fisiere catre tracker 
    MPI_Send(&number_of_files, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    
    // structura de date unde se salveaza swarmurile primite de la tracker 
    int **task_swarms = (int **) malloc(sizeof(int *) * number_of_wishes);
    
    for (unsigned int i = 0; i < number_of_wishes; i++) {
        task_swarms[i] = (int *) malloc(sizeof(int) * number_of_tasks);
    }
   
    
    for (unsigned int i = 0; i < number_of_files; i++) {
        // trimite numarul de segmente ale fisierului i
        MPI_Send(&initializationRequest[i].number_of_segments, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

        // trimite numele fisierului i 
        MPI_Send(initializationRequest[i].fileName, MAX_FILENAME, MPI_CHAR, 0, 0, MPI_COMM_WORLD);

        // trimite hash-urile segmentelor fisierului i
        for (unsigned int j = 0; j < initializationRequest[i].number_of_segments; j++) {
            MPI_Send(initializationRequest[i].hashes[j], HASH_SIZE, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        }
    }

    int ready_to_download = 0;
    int relevance = 0;
    int confirmation = 0;
    char *hash_to_retrieve = (char *) malloc(sizeof(char) * HASH_SIZE);
    char* current_wish = (char *) malloc(sizeof(char) * MAX_FILENAME);
    int finish = 0;

    // asteapta confirmarea de la tracker pentru descarcarea fisierelor dorite
    MPI_Recv(&ready_to_download, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, NULL);

    // deblocheaza threadul upload()
    pthread_mutex_unlock(argument.upload_mutex);

    while(true) {
        // verific daca trebuie sa trimit request catre tracker
        if (number_of_collected_segments % MAX_FILES == 0) {
            // pentru fiecare fisier dorit de client
            for (unsigned int i = 0; i < number_of_wishes; i++) {
                std::string current_wish = wish_list[i];

                // creez un nou request de tip UPDATE/FIRST CONTACT
                BitTorrentRequest newRequest;
                newRequest.client_id = rank;
                newRequest.desired_file = (char *) malloc(sizeof(char) * MAX_FILENAME);

                strcpy(newRequest.desired_file, current_wish.c_str());
                newRequest.request_type = UPDATE;

                // daca e prima data cand cer swarm pentru fisierul curent, setez FIRST_CONTACT
                if (!wished_files_situation[current_wish]) {
                    newRequest.request_type = FIRST_CONTACT;
                }

                // trimit id-ul clientului
                MPI_Send(&newRequest.client_id, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

                // trimit numele fisierului dorit
                MPI_Send(newRequest.desired_file, MAX_FILENAME, MPI_CHAR, 0, 0, MPI_COMM_WORLD);

                // trimit tipul de cerere de rezolvat
                MPI_Send(&newRequest.request_type, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

                // primesc swarmurile aici
                MPI_Recv(task_swarms[i], number_of_tasks, MPI_INT, 0, 0, MPI_COMM_WORLD, NULL);
                
                // verific daca nu am primit inca hash-urile pentru fisierul curent
                if (wished_files_situation[current_wish] == 0) {
                    saved_files[wished_files_received].fileName = (char *) malloc(sizeof(char) * MAX_FILENAME);
                    strcpy(saved_files[wished_files_received].fileName, current_wish.c_str());

                    // primesc numarul de segmente in care este impartit fisierul
                    MPI_Recv(&saved_files[wished_files_received].number_of_segments, 1, MPI_INT, 0, HASH_RECEIVE, MPI_COMM_WORLD, NULL);

                    // alocare dinamica pentru vectorul de hash-uri
                    saved_files[wished_files_received].hashes = (char **) malloc(sizeof(char *) * saved_files[wished_files_received].number_of_segments);

                    verified_hashes[i] = (int *) malloc(sizeof(int) * saved_files[wished_files_received].number_of_segments);

                    // primesc hashurile fisierului cerut
                    for (unsigned int j = 0; j < saved_files[wished_files_received].number_of_segments; j++) {
                        // initializez structura de verificare a descarcarii fisierelor de la clienti
                        verified_hashes[i][j] = 0;

                        // primesc hash-urile in ordine de la tracker
                        saved_files[wished_files_received].hashes[j] = (char *) malloc(sizeof(char) * HASH_SIZE);
                        MPI_Recv(saved_files[wished_files_received].hashes[j], HASH_SIZE, MPI_CHAR, 0, HASH_RECEIVE, MPI_COMM_WORLD, NULL);
                    }

                    // marchez ca am primit hash-urile de la tracker
                    wished_files_situation[current_wish] = 1;
                }
                wished_files_received++;
            }

            number_of_collected_segments = 0;
        }

        for (unsigned int i = 0; i < number_of_wishes; i++) {
            strcpy(current_wish, wish_list[i]);

            for (unsigned int j = 0; j < saved_files[i].number_of_segments; j++) {
                relevance = 0;
                // verific daca am segmentul de fisier descarcat 
                if (!verified_hashes[i][j]) {
                    strcpy(hash_to_retrieve, saved_files[i].hashes[j]);

                    for (int k = 0; k < number_of_tasks; k++) {
                        relevance = 1;

                        // evit rankurile mai mari decat poate suporta reteaua, fie nodul master, fie nodurile client din swarm care sunt la fel ca rankul lui
                        if (!task_swarms[i][k] || (task_swarms[i][k] > 0 && rank == task_swarms[i][k]) || (task_swarms[i][k] >= number_of_tasks)) {
                            relevance = 0;
                        }

                        if (!task_swarms[i][k]) {
                            continue;
                        }

                        if (relevance) {
                            // trimit rankul clientului
                            MPI_Send(&rank, 1, MPI_INT, task_swarms[i][k], UPLOAD_TAG_1, MPI_COMM_WORLD);
    
                            // trimite variabila de stare a procesului curent
                            MPI_Send(&finish, 1, MPI_INT, task_swarms[i][k], UPLOAD_TAG_1, MPI_COMM_WORLD);
                            
                            // trimite hashul segmentului de primit
                            MPI_Send(hash_to_retrieve, HASH_SIZE, MPI_CHAR, task_swarms[i][k], UPLOAD_TAG_1, MPI_COMM_WORLD);

                            // trimite fisierul sursa de unde provine segmentul dorit de client
                            MPI_Send(current_wish, MAX_FILENAME, MPI_CHAR, task_swarms[i][k], UPLOAD_TAG_1 , MPI_COMM_WORLD);
                
                            // primeste confirmarea de la uploader

                            MPI_Recv(&confirmation, 1, MPI_INT, task_swarms[i][k], UPLOAD_TAG, MPI_COMM_WORLD, NULL);

                            if (confirmation == 1) {
                                verified_hashes[i][j] = 1;
                                break;
                            }
                        }
                    }
                }
            }
        }
        number_of_collected_segments += 1;
        // verific daca am toate segmentele in toate fisierele dorite de proces

        int has_all_segments = 0;
        int has_files = 1;

        for (unsigned int i = 0; i < number_of_wishes; i++) {
            std::string wish = wish_list[i];

            has_all_segments = 1;
            for (unsigned int j = 0; j < saved_files[i].number_of_segments; j++) {
                if (!verified_hashes[i][j]) {
                    has_all_segments = 0;
                    
                    break;
                }
            }

            if (has_all_segments == 1 && !downloaded_files_situation[saved_files[i].fileName]) {
                // putem anunta trackerul ca am descarcat intreg fisierul 

                BitTorrentRequest finishedOneFileRequest;
                finishedOneFileRequest.client_id = rank;
                finishedOneFileRequest.desired_file = saved_files[i].fileName;
                finishedOneFileRequest.request_type = FINISH_DOWNLOADING_ONE_FILE;
                downloaded_files_situation[saved_files[i].fileName] = 1;

                MPI_Send(&finishedOneFileRequest.client_id, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
                MPI_Send(finishedOneFileRequest.desired_file, MAX_FILENAME, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
                MPI_Send(&finishedOneFileRequest.request_type, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

                string fileName = saved_files[i].fileName;
                string output_file = "client" + to_string(rank) + "_" + fileName;
                
                ofstream out(output_file);

                for (unsigned int j = 0; j < saved_files[i].number_of_segments; j++) {
                    out << saved_files[i].hashes[j] << endl;
                }
            } 
        }

        for (auto &value : downloaded_files_situation) {
            if (!value.second) {
                has_files = 0;
                break;
            }
        }

        if (has_files) {
            
            *argument.finished_job = 1;

            BitTorrentRequest finishedFilesRequest;
            finishedFilesRequest.client_id = rank;
            finishedFilesRequest.desired_file = (char*) "irelevant";
            finishedFilesRequest.request_type = FINISH_DOWNLOADING_ALL_FILES;

            MPI_Send(&finishedFilesRequest.client_id, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            MPI_Send(finishedFilesRequest.desired_file, MAX_FILENAME, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
            MPI_Send(&finishedFilesRequest.request_type, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

            int close_download_thread = 0;
            MPI_Recv(&close_download_thread, 1, MPI_INT, 0, FINISH_DOWNLOADING_ALL_FILES, MPI_COMM_WORLD, NULL);
            // close this thread

            if (close_download_thread) {
                break;
            }
        }
    }

    return NULL;
}

void *upload_thread_func(void *arg)
{
    Arguments argument = *(Arguments*) arg;
    pthread_mutex_lock(argument.upload_mutex);

    unsigned int number_of_files = *argument.number_of_files, number_of_wishes = *argument.number_of_wishes;

    InitializationRequest *initializationRequest = *argument.initializationRequest;
    InitializationRequest *saved_files = *argument.saved_files;

    int **verified_hashes = *argument.verified_hashes;
    int confirmation = 0;
    int current_client = 0;

    while(1) {
        char *hash_to_accept = (char *) malloc(sizeof(char) * HASH_SIZE);
        char *file_source = (char *) malloc(sizeof(char) * MAX_FILENAME);

        confirmation = 0;
        
        int finish_uploading = 0;

        MPI_Recv(&current_client, 1, MPI_INT, MPI_ANY_SOURCE, UPLOAD_TAG_1, MPI_COMM_WORLD, NULL);
        MPI_Recv(&finish_uploading, 1, MPI_INT, current_client, UPLOAD_TAG_1, MPI_COMM_WORLD, NULL);

        if (finish_uploading == 1) {
            break;
        }

        MPI_Recv(hash_to_accept, HASH_SIZE, MPI_CHAR, current_client, UPLOAD_TAG_1, MPI_COMM_WORLD, NULL);
        MPI_Recv(file_source, MAX_FILENAME, MPI_CHAR, current_client, UPLOAD_TAG_1, MPI_COMM_WORLD, NULL);

        // check the owned files
        for (unsigned int j = 0; j < number_of_files; j++) {
            InitializationRequest currentFileStructure = initializationRequest[j];
            if (!strcmp(currentFileStructure.fileName, file_source)) {
                for (unsigned int k = 0; k < currentFileStructure.number_of_segments; k++) {
                    if (!strcmp(hash_to_accept, currentFileStructure.hashes[k])) {
                        confirmation = 1;
                        break;
                    }
                }
            }
        }

        if (!confirmation) {
            // check the gained files so far
            for (unsigned int j = 0; j < number_of_wishes; j++) {
                InitializationRequest currentFileStructure = saved_files[j];

                if (!strcmp(currentFileStructure.fileName, file_source)) {
                    for (unsigned int k = 0; k < currentFileStructure.number_of_segments; k++) {
                        if (!strcmp(hash_to_accept, currentFileStructure.hashes[k]) && verified_hashes[j] != NULL) {
                            if(verified_hashes[j][k] == 1) {
                                confirmation = 1;
                                break;
                            }
                        }
                    }
                }
            }
        }

        MPI_Send(&confirmation, 1, MPI_INT, current_client, UPLOAD_TAG, MPI_COMM_WORLD);
    }

    return NULL;
}

void print_swarms(std::map<string, vector<ClientInfo>> trackerSwarmsTracking) {
    for (auto &value : trackerSwarmsTracking) {
        cout << value.first << " ";
        for (unsigned int i = 0; i < value.second.size(); i++) {
            cout << value.second[i].client_id << " " << value.second[i].client_state << endl;
        }
        cout << endl;
    }
}

void tracker(int numtasks, int rank){
    int number_of_files = 0;
    
    std::vector<CLIENT_TERMINATION_STATE> clientsTerminationStates(numtasks, NOT_TERMINATED);
    MPI_Status new_status;
    std::set<string> desiredFiles;
    int *clients = (int *) malloc(sizeof(int) * numtasks);
    std::map<string, vector<ClientInfo>> trackerSwarmsTracking;

    TrackerDatabaseEntry *trackerDatabaseEntry = (TrackerDatabaseEntry *) malloc(sizeof(TrackerDatabaseEntry) * numtasks);

    for (int i = 1; i < numtasks; i++) {
        MPI_Recv(&number_of_files, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &new_status);

        trackerDatabaseEntry[i].client_id = i;
        trackerDatabaseEntry[i].number_of_files = number_of_files;

        if (trackerDatabaseEntry[i].number_of_files > 0) {
            trackerDatabaseEntry[i].fileMetadata = (InitializationRequest *) malloc(sizeof(InitializationRequest) * trackerDatabaseEntry[i].number_of_files);

            for (unsigned int j = 0; j < trackerDatabaseEntry[i].number_of_files; j++) {
                // primeste numarul de segmente
                MPI_Recv(&trackerDatabaseEntry[i].fileMetadata[j].number_of_segments, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &new_status);

                // primeste numele fisierului detinut de client
                trackerDatabaseEntry[i].fileMetadata[j].fileName = (char *) malloc(sizeof(char) * MAX_FILENAME);
                MPI_Recv(trackerDatabaseEntry[i].fileMetadata[j].fileName, MAX_FILENAME, MPI_CHAR, i, 0, MPI_COMM_WORLD, NULL);

                // se umple lista cu fisiere ce se doresc distribuite in sistem 
                desiredFiles.insert(trackerDatabaseEntry[i].fileMetadata[j].fileName);

                // primeste hashurile segmentelor din fisierul detinut de client
                trackerDatabaseEntry[i].fileMetadata[j].hashes = (char **) malloc(sizeof(char *) * trackerDatabaseEntry[i].fileMetadata[j].number_of_segments);
                
                for (unsigned int k = 0; k < trackerDatabaseEntry[i].fileMetadata[j].number_of_segments; k++) {
                    trackerDatabaseEntry[i].fileMetadata[j].hashes[k] = (char *) malloc(sizeof(char) * HASH_SIZE);
                }

                for (unsigned int k = 0; k < trackerDatabaseEntry[i].fileMetadata[j].number_of_segments; k++) {
                    MPI_Recv(trackerDatabaseEntry[i].fileMetadata[j].hashes[k], HASH_SIZE, MPI_CHAR, i, 0, MPI_COMM_WORLD, &new_status);
                }
            }
        } 
    }

    for (std::set<string> ::iterator it = desiredFiles.begin(); it != desiredFiles.end(); it++) {
        std::string newEntryKey = *it;
   
        std::vector<ClientInfo> newEntryValue;
        
        for (int i = 1; i < numtasks; i++) {
            TrackerDatabaseEntry currentDatabaseEntry = trackerDatabaseEntry[i];
            
            ClientInfo newSwarmMember;
            newSwarmMember.client_id = i;
            
            InitializationRequest *currentRequestsToAnalyze = currentDatabaseEntry.fileMetadata;

            bool is_owner = false;

            for (unsigned int j = 0; j < currentDatabaseEntry.number_of_files; j++) {
                if (!strcmp(newEntryKey.c_str(), currentRequestsToAnalyze[j].fileName)) {
                    is_owner = true;
                } 
            }

            if (!is_owner) {
                newSwarmMember.client_state = LEECHER;
            } else {
                newSwarmMember.client_state = SEED;
            }

            newEntryValue.push_back(newSwarmMember);
        }

        trackerSwarmsTracking[newEntryKey] = newEntryValue;
    }

    /* tracker sending acks to the processes from the network */
    for (int i = 1; i < numtasks; i++) {
        int confirmation = 1;
        MPI_Send(&confirmation, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
    }

    //clientii de trimis catre client
    int *new_clients = (int *) malloc(sizeof(int) * numtasks);

    // numele fisierului dorit de client
    char *pending_file = (char *) malloc(sizeof(*pending_file) * MAX_FILENAME);

    // tracker-ul intra in tracker loop destinat servirii clientilor cu informatiile initiale
    while (1) {
        bool terminationToDo = true;

        for (unsigned int i = 1; i < clientsTerminationStates.size(); i++) {
            if (clientsTerminationStates[i] == NOT_TERMINATED) {
                terminationToDo = false;
                break;
            }
        }

        int finish = 1;
        if (terminationToDo) {            
            for (int i = 1; i < numtasks; i++) {
                MPI_Send(&rank, 1, MPI_INT, i, UPLOAD_TAG_1, MPI_COMM_WORLD);
                MPI_Send(&finish, 1, MPI_INT, i, UPLOAD_TAG_1, MPI_COMM_WORLD);
            }

            break;
        }

        int next_client_id = 0, request_type = 0;

        // primeste id-ul clientului cu cererea curenta
        MPI_Recv(&next_client_id, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, NULL);

        // primeste fisierul dorit de client
        MPI_Recv(pending_file, MAX_FILENAME, MPI_CHAR, next_client_id, 0, MPI_COMM_WORLD, NULL);

        // primeste tipul de request trimis de la client
        MPI_Recv(&request_type, 1, MPI_INT, next_client_id, 0, MPI_COMM_WORLD, NULL);

        // construieste lista cu clientii recomandati (preia de aici si construieste un vector de intregi 
        // usor de trimis in retea

        if (request_type == FINISH_DOWNLOADING_ALL_FILES) {
            clientsTerminationStates[next_client_id] = TERMINATED;

            int close_download_thread = 1;
            MPI_Send(&close_download_thread, 1, MPI_INT, next_client_id, FINISH_DOWNLOADING_ALL_FILES, MPI_COMM_WORLD);

            continue;
        } else if (request_type == FINISH_DOWNLOADING_ONE_FILE) {
            std::vector<ClientInfo>& recommendedClients = trackerSwarmsTracking[pending_file];
            recommendedClients[next_client_id].client_state == SEED;
            continue;
        }

        std::vector<ClientInfo> recommendedClients = trackerSwarmsTracking[pending_file];

        for (unsigned int j = 0; j < recommendedClients.size(); j++) {
            int current_client = recommendedClients[j].client_id;
            if (recommendedClients[j].client_state == SEED || recommendedClients[j].client_state == PEER) {
                clients[j] = current_client;
            } else {
                clients[j] = 0; // invalid client = master
            }
        }

        // optimizare: calculez numarul de fisiere pe care clientul cu ordinul i le contine (aka daca e PEER/SEED), atunci e mai predispus la a fi ocupat

        // sortarea vreau sa fie liniara, fara sa tot fac switch-uri pe, cel mai probabil, putine fisiere (counting sort)
        vector<int> freq_array(MAX_FILES, 0);

        for (unsigned int i = 0; i < recommendedClients.size(); i++) {
            if (clients[i] != 0) {
                int deserving_files = 0;
                for (auto &value : trackerSwarmsTracking) {
                    for (ClientInfo clientInfo : value.second) {
                        if (clientInfo.client_id == clients[i]) {
                            if (clientInfo.client_state == PEER || clientInfo.client_state == SEED) {
                                deserving_files += 1;
                            }
                        }
                    }
                }
                
                if (deserving_files) {
                    freq_array[deserving_files] = clients[i];    
                }
            }
        }

        for (int i = 0; i < numtasks; i++) {
            new_clients[i] = 0;
        }

        int clients_deserving = 0;
        for (int i = 0; i < MAX_FILES; i++) {
            if (freq_array[i]) {
                new_clients[clients_deserving] = freq_array[i];
                clients_deserving++;
            }
        }

        MPI_Send(new_clients, numtasks, MPI_INT, next_client_id, 0, MPI_COMM_WORLD);
        
        if (recommendedClients[(unsigned int)next_client_id].client_state == LEECHER) {
            recommendedClients[(unsigned int)next_client_id].client_state = PEER;
        }
        

        if (request_type == FIRST_CONTACT) {
            bool info_received = false;
            for (int j = 1; j < numtasks; j++) {
                for (unsigned int k = 0; k < trackerDatabaseEntry[j].number_of_files; k++) {
                    if (!strcmp(trackerDatabaseEntry[j].fileMetadata[k].fileName, pending_file)) {
                        MPI_Send(&trackerDatabaseEntry[j].fileMetadata[k].number_of_segments, 1, MPI_INT, next_client_id, HASH_RECEIVE, MPI_COMM_WORLD);
                        
                        for (unsigned int t = 0; t < trackerDatabaseEntry[j].fileMetadata[k].number_of_segments; t++) {
                            MPI_Send(trackerDatabaseEntry[j].fileMetadata[k].hashes[t], HASH_SIZE, MPI_CHAR, next_client_id, HASH_RECEIVE, MPI_COMM_WORLD);
                        }

                        info_received = true;
                        break;
                    }

                }

                if (info_received == true) {
                    break;
                }
            }
        }
    }

    // eliberez resursele consumate de tracker

    for (int i = 1; i < numtasks; i++) {
        for (unsigned int j = 0; j < trackerDatabaseEntry[i].number_of_files; j++) {
            for (unsigned int k = 0; k < trackerDatabaseEntry[i].fileMetadata[j].number_of_segments; k++) {
                free(trackerDatabaseEntry[i].fileMetadata[j].hashes[k]);
            }
            free(trackerDatabaseEntry[i].fileMetadata[j].hashes);
        }
    }

    free(trackerDatabaseEntry);
    free(clients);
    free(pending_file);
    free(new_clients);

    for (auto &value : trackerSwarmsTracking) {
        value.second.clear();
    }
    trackerSwarmsTracking.clear();
}

void clear_metadata(InitializationRequest **deleted_list, int size) {
    for (int i = 0; i < size; i++) {
        for (unsigned int j = 0; j < (*deleted_list)[i].number_of_segments; j++) {
            free((*deleted_list)[i].hashes[j]);
        }

        free((*deleted_list)[i].hashes);
    }
    free((*deleted_list));
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    pthread_mutex_t upload;
    void *status;
    int r;

    if (pthread_mutex_init(&upload, NULL) != 0) {
        printf("Error initializing mutex\n");
        exit(-1);
    }

    pthread_mutex_lock(&upload);

    unsigned int number_of_files, number_of_wishes = 0;
    char **wish_list;
    InitializationRequest *initializationRequest = read_file_retrieve_info(rank, &number_of_files, &wish_list, &number_of_wishes);
    InitializationRequest *saved_files = (InitializationRequest *) malloc(sizeof(InitializationRequest) * number_of_wishes);
    int **verified_hashes = (int **) malloc(sizeof(int *) * number_of_wishes);
    int finished_job = 0;

    Arguments newArgument {
        .upload_mutex = &upload,
        .rank = rank,
        .number_of_tasks = (unsigned int)numtasks,
        .initializationRequest = &initializationRequest,
        .saved_files = &saved_files,
        .wish_list = &wish_list,
        .number_of_files = &number_of_files,
        .number_of_wishes = &number_of_wishes,
        .verified_hashes = &verified_hashes,
        .finished_job = &finished_job,
    };

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &newArgument);

    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &newArgument);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);

    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }

    // eliberez resursele alocate de peer

    clear_metadata(&initializationRequest, number_of_files);
    clear_metadata(&saved_files, number_of_wishes);

    for (unsigned int i = 0; i < number_of_wishes; i++) {
        free(wish_list[i]);
        free(verified_hashes[i]);
    }
    
    free(verified_hashes);
    free(wish_list);
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }

    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}