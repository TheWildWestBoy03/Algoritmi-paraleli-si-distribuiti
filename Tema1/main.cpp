#include <pthread.h>
#include <iostream>
#include <stdlib.h>
#include <map>
#include <vector>
#include <cstring>
#include <algorithm>
#include <map>
#include <math.h>
#include <unistd.h>
#include <set>
#include <fstream>
#include <sstream>

#define NUM_LETTERS 25

using namespace std;

/* structura pentru organizarea mai facila a fisierelor ce vor fi sortate inainte de pornirea threadurilor */
struct AggregatedInfo {
    std::vector<unsigned int> file_ids;
    std::string name;
};

/* informatii(de baza) legate de fisier */
struct FileInfo {
    unsigned int id;
    unsigned long file_size;
    std::string file_name;
};

/*structura argumentului pentru mappere */
struct MapperThreadArgument {
    unsigned long thread_id;            /* id-ul thread-ului*/
    unsigned long lower_bound;          /* limita de jos a fisierelor asignate threadului */
    unsigned long upper_bound;          /* limita de sus */
    std::vector<FileInfo*> fileInfos;      /* vector de structuri de informatii de fisiere */
    pthread_barrier_t *mapper_barrier;      /* bariera folosita pentru ordinea mapper -> reducer */
    std::map<unsigned int, std::set<string>> *thread_map;   /* structura shared a threadurilor map*/
    pthread_mutex_t *dictionary_mutex;      /* mutex folosit pentru citirea fisierelor */
};

/* structura argumentului pentru reducer*/
struct ReducerThreadArgument {
    unsigned long thread_id;            /* id-ul thread-ului */
    pthread_barrier_t *reducer_barrier;     /* bariera folosita pentru ordinea mapper->reducer*/
    std::map<std::string, std::set<unsigned int>> *word_id_reducer_dict;    /* structura de liste agregate */
    std::map<unsigned int, std::set<std::string>> *mapper_result;   /* rezultatul primit de la mappere*/

    pthread_mutex_t *convertion_mutex;          /* mutex-ul reducerului */
    pthread_barrier_t *convertion_barrier;      /*bariera reducerului */

    unsigned long lower_bound;                  /* limita de jos pentru ordinea cuvantului de unde se incepe prelucrarea individuala */
    unsigned long upper_bound;                  /* limita de su s*/

    unsigned long alphabet_lower_bound;         /* litera cu care se incepe prelucrarea individuala */
    unsigned long alphabet_upper_bound;         /* litera cu care se termina prelucrarea individuala */

    unsigned long id_lower_bound;               /* cel mai mic file id destinat reducerului */
    unsigned long id_upper_bound;                  /* cel mai mare file id */

    unsigned long string_lower_bound;           
    unsigned long string_upper_bound;

    std::vector<AggregatedInfo> *temporaryArray;        /* vectorul obtinut din listele agregate */
    int number_of_reducers;
    int number_of_mappers;

    std::map<unsigned int, std::string> *alphabet_file_map;         /* lista de fisiere tip out */
    std::vector<unsigned long> *word_freq;                          /* frecventa cuvintelor */
};

/**
 * aceasta functie returneaza lista de nume de fisiere din fisierul de input dat ca parametru.
 */
std::vector<string> getFileList(char *test_file_name) {
    FILE *testFilePointer = fopen(test_file_name, "rt");
    std::vector<string> files;

    char *currentFileName = (char *) malloc(sizeof(*currentFileName) * 200);
    if (testFilePointer == NULL) {
        cout << "The file couldn't be open. Exiting!" << endl;
        exit(-1);
    }

    /* traversarea fisierului */
    while (feof(testFilePointer) == 0) {
        fgets(currentFileName, 199, testFilePointer);

        /* omitem primul numar din fisier */
        if (atoi(currentFileName) != 0) {
            continue;
        }

        /* salvam linia curenta in vetor */
        if (feof(testFilePointer) == 0) {
            currentFileName[strlen(currentFileName) - 1] = '\0';
        }

        std::string new_file_string = currentFileName;
        files.push_back(new_file_string);

        currentFileName[0] = '\0';
    }

    return files;
}

/**
 * aceasta functie returneaza un vector de elemente ce reprezinta informatii esentiale aferente fisierelor de prelucrat
 */
std::vector<FileInfo *> getFileSizes(std::vector<string> filenames, unsigned long *totalSum) {
    std::vector<FileInfo *> fileInfos;
    fileInfos.resize(filenames.size());

    for (unsigned int i = 0; i < filenames.size(); i++) {
        FILE* currentFile = fopen(filenames[i].c_str(), "rt");

        if (currentFile == NULL) {
            cout << "FILE DOESN'T EXIST";
            exit(-1);
        }

        /* folosesc fseek catre finalul fisierului ca sa obtin size-ul acestuia */
        int r = fseek(currentFile, 0, SEEK_END);

        if (r < 0) {
            perror("the fseek operation failed");
            exit(-1);
        }

        FileInfo *newFileInfoNode = (FileInfo *) malloc(sizeof(FileInfo));

        newFileInfoNode->file_name = filenames[i];
        newFileInfoNode->file_size = ftell(currentFile);
        newFileInfoNode->id = i;
    
        fileInfos[i] = newFileInfoNode;

        (*totalSum) += newFileInfoNode->file_size;
        fclose(currentFile);
    }

    return fileInfos;
}

bool comparingFunction(FileInfo* fileInfo1, FileInfo *fileInfo2) {
    return fileInfo2->file_size < fileInfo1->file_size;
}

/**
 * se creeaza un vector de argumente ce va fi folosit de toti mapperii
 */

std::vector<MapperThreadArgument> CreateArgumentArray (float totalSum, unsigned int numberOfMappers, float sizeOnMapper, std::vector<FileInfo *> &fileInfo,
                                                            pthread_barrier_t barrier) {
    std::vector<MapperThreadArgument> arguments;

    sort(fileInfo.begin(), fileInfo.end(), comparingFunction);
    float currentSum = 0.0f;
    unsigned long firstFile = 0;

    unsigned long currentMapperToAssign = 0;
    for (unsigned long i = 0; i < fileInfo.size(); i++) {
        currentSum += fileInfo[i]->file_size;
        /*verific daca dimensiunea cumulata a ultimelor fisiere depasesc pragul definit in functia main */
        if (currentSum >= sizeOnMapper) {
            MapperThreadArgument newArgument;
        
            newArgument.lower_bound = firstFile;
            newArgument.upper_bound = i;
            newArgument.thread_id = currentMapperToAssign;
            newArgument.fileInfos = fileInfo;
            newArgument.mapper_barrier = &barrier;
            firstFile = i + 1;
            currentSum = 0.0f;

            arguments.push_back(newArgument);
            currentMapperToAssign += 1;
            
        } 
    }

    /* ma asigur ca si ultimele fisiere au fost procesate in acelasi mods*/
    if (numberOfMappers > 1) {
        unsigned long final_bound = fileInfo.size() - 1;

        MapperThreadArgument newArgument;
        newArgument.lower_bound = firstFile;
        newArgument.upper_bound = final_bound;
        newArgument.fileInfos = fileInfo;
        newArgument.mapper_barrier = &barrier;
        newArgument.thread_id = currentMapperToAssign;
        arguments.push_back(newArgument);
    }

    return arguments;
}

void *map_function(void *arg) {
    MapperThreadArgument *arguments = (MapperThreadArgument *) arg;
    std::vector<FileInfo *> fileInfos = arguments->fileInfos;

    unsigned long lower_bound = arguments->lower_bound;
    unsigned long upper_bound = arguments->upper_bound;

    for (long unsigned i = lower_bound; i <= upper_bound; i++) {
        std::string current_file_name = fileInfos[i]->file_name;
        int file_id = fileInfos[i] -> id;

        fstream file;
        string token;
        
        file.open(current_file_name.c_str());
    
        /* incep citirea efectiva a cuvintelor din fisierul de ordin i */
        pthread_mutex_lock(arguments->dictionary_mutex);
        while (file >> token) {
            /* sterg caracterele non-alfabetice */
            token.erase(std::remove_if(token.begin(), token.end(), [](char c) {
                return !isalpha(c);
            }), token.end());

            /* transform tokeul in lowercase*/
            std::transform(token.begin(), token.end(), token.begin(), [](char c) { 
                return std::tolower(c);
            });

            /* adaug noul cuvant la structura comuna */
            if (token.size() > 0) {
                (*arguments->thread_map)[file_id + 1].insert(token);
            }            
        }
        pthread_mutex_unlock(arguments->dictionary_mutex);

    }

    pthread_barrier_wait(arguments -> mapper_barrier);

    return NULL;
}


void file_map_to_word_map(std::map<unsigned int, std::set<string>> id_word_map, std::map<string, std::set<unsigned int>> *final_map, std::vector<unsigned long> *freq,
                                long lower_bound, long upper_bound, pthread_mutex_t *mutex) {

    for (long int i = lower_bound; i < upper_bound; i++) {
        if ((long unsigned int)i == id_word_map.size()) {
            break;
        }
        auto it = id_word_map.begin();
        std::advance(it, i);

        pthread_mutex_lock(mutex);
        for (const std::string &value : it->second) {
            std::set<unsigned int> local_set;
            auto &vec = (*final_map)[value];
            if (std::find(vec.begin(), vec.end(), it->first) == vec.end()) {
                vec.insert(it->first);
            }
        }
        pthread_mutex_unlock(mutex);
    }

}

/* returneaza frecventele cuvintelor in functie de litera de inceput */
void create_frequencies(std::map<string, std::set<unsigned int>> map, std::vector<unsigned long> *freq, long lower_bound, long upper_bound) {

    for (int i = lower_bound; i < upper_bound; i++) {
        auto it = map.begin();
        std::advance(it, i);

        (*freq)[it->first[0] - 97]++;    
    }
}

/* rezulta un vector de sume partiale ale frecventelor calculate anterior */
void finish_freq(std::vector<unsigned long> *freq) {
    for (long unsigned int i = 1; i < (*freq).size(); i++) {
        (*freq)[i] += (*freq)[i - 1];
    }
}

void *reducer_function(void *arg) {
    ReducerThreadArgument *arguments = (ReducerThreadArgument *) arg;

    pthread_barrier_wait(arguments -> reducer_barrier);
    /* de aici stim ca mappere-le s-au terminat, iar reducerii tocmai au inceput */
    
    long result_map_length = arguments->mapper_result->size(), aggregated_length = 0;

    /* calculez limitele fisierelor din care reducerii vor forma liste agregate */
    arguments->id_lower_bound = (arguments->thread_id - arguments->number_of_mappers)* (double) result_map_length / arguments->number_of_reducers;
    arguments->id_upper_bound = fmin((arguments->thread_id - arguments->number_of_mappers + 1) * (double) result_map_length / arguments->number_of_reducers, result_map_length);

    // agregarea listei obtinute din mapper
    file_map_to_word_map(*arguments->mapper_result, arguments->word_id_reducer_dict, arguments->word_freq, arguments->id_lower_bound, arguments->id_upper_bound, arguments->convertion_mutex);
    
    pthread_barrier_wait(arguments->convertion_barrier);

    /* bound-uri cu care calculez frecventele cuvintelor in functie de litera cu care incep */
    aggregated_length = arguments->word_id_reducer_dict->size();
    arguments->string_lower_bound = (arguments->thread_id - arguments->number_of_mappers)* (double) aggregated_length / arguments->number_of_reducers;
    arguments->string_upper_bound = fmin((arguments->thread_id - arguments->number_of_mappers + 1) * (double) aggregated_length / arguments->number_of_reducers, aggregated_length);

    create_frequencies(*arguments->word_id_reducer_dict, arguments->word_freq, arguments->string_lower_bound, arguments->string_upper_bound);

    pthread_barrier_wait(arguments->convertion_barrier);

    unsigned long map_length = arguments->word_id_reducer_dict->size();
    if (arguments->thread_id == (unsigned long)arguments->number_of_mappers) {
        arguments->temporaryArray->resize(map_length);
        finish_freq(arguments->word_freq);
    }

    pthread_barrier_wait(arguments->convertion_barrier);
    
    /* calculez limitele cu care reducerii vor lucra mai departe */
    arguments->alphabet_lower_bound = (arguments->thread_id - arguments->number_of_mappers)* (double) NUM_LETTERS / arguments->number_of_reducers;
    arguments->alphabet_upper_bound = fmin((arguments->thread_id - arguments->number_of_mappers + 1) * (double) NUM_LETTERS / arguments->number_of_reducers, NUM_LETTERS);

    if (arguments->thread_id == (unsigned long)arguments->number_of_mappers) {
        arguments->lower_bound = 0;
    } else {
        arguments->lower_bound = (*arguments->word_freq)[arguments->alphabet_lower_bound - 1];
    }

    arguments->upper_bound = (*arguments->word_freq)[arguments->alphabet_upper_bound];

    /* adaugarea paralela a intrarilor din lista agregata in vectorul partajat */
    for (unsigned long i = arguments->lower_bound; i < arguments->upper_bound; i++) {
        auto it = arguments->word_id_reducer_dict->begin();
        std::advance(it, i);

        AggregatedInfo newInfo;
        newInfo.name = it->first;

        newInfo.file_ids.insert(newInfo.file_ids.end(), it->second.begin(), it->second.end());
        (*arguments->temporaryArray)[i] = newInfo;
    }
    
    /* sortarea pe bucati a vectorului partajat */
    if (arguments->lower_bound < arguments->upper_bound) {
        std::sort(
            arguments->temporaryArray->begin() + arguments->lower_bound,
            arguments->temporaryArray->begin() + arguments->upper_bound,
            [](const AggregatedInfo &a, const AggregatedInfo &b) {
                if (a.name[0] != b.name[0])
                    return a.name < b.name;
                if (a.file_ids.size() == b.file_ids.size()) {
                    return a.name < b.name;
                }
                return a.file_ids.size() > b.file_ids.size();
            });
    }

    long writing_lower_bound = arguments->lower_bound;
    long writing_upper_bound = arguments->upper_bound;

    /* scrierea finala a intrarilor din vectorul sortat partajat in fisierele de iesire asignate fiecarui reducer */
    for (unsigned long int i = arguments->alphabet_lower_bound; i <= arguments->alphabet_upper_bound; i++) {
        FILE *f = fopen((*arguments->alphabet_file_map)[i].c_str(), "w");

        if (f == NULL) {
            return NULL;
        }

        for (long j = writing_lower_bound; j < writing_upper_bound; j++) {
            AggregatedInfo current = (*(arguments->temporaryArray))[j];
            if (current.name[0] == (char)(i + 97)) {
                std::string line = current.name + ":[";

                if (!current.file_ids.empty()) {
                    for (size_t k = 0; k < current.file_ids.size(); ++k) {
                        line += std::to_string(current.file_ids[k]) + (k + 1 < current.file_ids.size() ? " " : "");
                    }
                }

                line += "]\n";
                fwrite(line.c_str(), 1, line.size(), f);
            }
        }

        fclose(f);
    }

    return NULL;
}

/* crearea vectorului cu numele fisierelor de iesire in functie de tipul de test */
std::map<unsigned int, std::string> fill_dictionary(int numberOfMappers, int numberOfReducers) {
    std::map<unsigned int, std::string> dictionary;
    std::string test_folder = "test_par/";
    
    if (numberOfMappers == 1 && numberOfReducers == 1) {
        test_folder = "test_sec/";
    }

    for (int i = 0; i <= NUM_LETTERS; i++) {
        dictionary[i] = test_folder + (char)(i + 97) + ".txt";
    }

    return dictionary;
}

int main(int argc, char **argv)
{
    int r;
    unsigned long totalSum = 0;
    int number_of_mappers = atoi(argv[1]), number_of_reducers = atoi(argv[2]);
    void *status;
    int number_of_threads = number_of_mappers + number_of_reducers;

    std::map<string, std::set<unsigned int>> word_to_id_dictionary;   /* lista agregata partajata */
    std::map<unsigned int, std::set<string>> dictionary;    /*rezultatul mapperelor */
    std::map<unsigned int, std::string> alphabet_dictionary = fill_dictionary(number_of_mappers, number_of_reducers);   /*map-ul de nume de fisiere de iesire */
    std::vector<unsigned long> word_frequencies(NUM_LETTERS + 1, 0);

    pthread_barrier_t new_barrier;
    int rr = pthread_barrier_init(&new_barrier, NULL, number_of_threads);
    if (rr < 0) {
        exit(-1);
    }
    unsigned long ids[number_of_threads];
    pthread_t threads[number_of_threads];

    for (unsigned long i = 0; i < (unsigned long)number_of_threads; i++) {
        ids[i] = i;
    }

    pthread_barrier_t convertion_reducer_barrier;
    rr = pthread_barrier_init(&convertion_reducer_barrier, NULL, number_of_reducers);

    pthread_barrier_t sorting_barrier;
    rr = pthread_barrier_init(&sorting_barrier, NULL, number_of_reducers);

    pthread_mutex_t map_mutex;
    pthread_mutex_init(&map_mutex, NULL);

    pthread_mutex_t convertion_reducer_mutex;
    pthread_mutex_init(&convertion_reducer_mutex, NULL);

    std::vector<string> fileNames = getFileList(argv[3]);
    std::vector<FileInfo *> fileSizes = getFileSizes(fileNames, &totalSum);
    float sizeOnMapper = (float) totalSum / number_of_mappers;

    std::vector<MapperThreadArgument> mapperArguments = CreateArgumentArray(totalSum, number_of_mappers, sizeOnMapper, fileSizes, new_barrier);
    std::vector<ReducerThreadArgument> reducerArguments;

    std::vector<AggregatedInfo> temporaryArray;

    int alphabet_raport = 26 / number_of_reducers;
    int alphabet_lower_bound = 0, alphabet_upper_bound = alphabet_raport;

    for (int i = 0; i < number_of_reducers; i++) {
        ReducerThreadArgument argument;

        argument.reducer_barrier = &new_barrier;
        argument.thread_id = ids[i + number_of_mappers];
        argument.word_id_reducer_dict = &word_to_id_dictionary;
        argument.mapper_result = &dictionary;
        argument.convertion_mutex = &convertion_reducer_mutex;
        argument.convertion_barrier = &convertion_reducer_barrier;
        argument.word_freq = &word_frequencies;
        argument.number_of_reducers = number_of_reducers;
        argument.temporaryArray = &temporaryArray;
        argument.number_of_mappers = number_of_mappers;
        argument.alphabet_file_map = &alphabet_dictionary;

        if (i == 0) {
            argument.alphabet_lower_bound = alphabet_lower_bound;
            argument.alphabet_upper_bound = alphabet_upper_bound;
        } else {
            argument.alphabet_lower_bound = reducerArguments[reducerArguments.size() - 1].alphabet_upper_bound + 1;
            argument.alphabet_upper_bound = argument.alphabet_lower_bound + alphabet_raport;
        }

        reducerArguments.push_back(argument);
    }

    for (int id = 0; id < number_of_threads; id++) {
        if (id < number_of_mappers) {
            mapperArguments[id].mapper_barrier = &new_barrier;
            mapperArguments[id].thread_map = &dictionary;
            mapperArguments[id].dictionary_mutex = &map_mutex;
            r = pthread_create(&threads[id], NULL, map_function, &mapperArguments[id]);

            if (r != 0) {
                cout << "Eroare la crearea mapperului" << endl;
                exit(-1);
            } 
        } else {
            r = pthread_create(&threads[id], NULL, reducer_function, &reducerArguments[id - number_of_mappers]);

            if (r != 0) {
                cout << "Eroare la crearea reducerului" << endl;
                exit(-1);
            } 
        }
    }

    for (long id = 0; id < number_of_threads; id++) {
        r = pthread_join(threads[id], &status);

        if (r) {
            cout << "Eroare la join-uirea threadului" << endl;
            exit(-1);
        }
    }

    pthread_barrier_destroy(&new_barrier);
    pthread_barrier_destroy(&sorting_barrier);
    pthread_barrier_destroy(&convertion_reducer_barrier);
    pthread_mutex_destroy(&map_mutex);
    pthread_mutex_destroy(&convertion_reducer_mutex);
    word_to_id_dictionary.clear();
    alphabet_dictionary.clear();
    mapperArguments.clear();
    reducerArguments.clear();
    fileNames.clear();
    dictionary.clear();

    return 0;
}