#include <assert.h>
#include <stdint.h>
#include <iomanip>
#include <sstream>
#include <cstring>
#include <string>
#include <vector>
#include <map>
#include <math.h>
#include <stdio.h>
#ifdef WIN32
#include <io.h>
#endif
#include <fcntl.h>
#include <sha1.h>
#include "log.hpp"

#include "MyService.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <transport/TSocket.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace  ::mp2;

using namespace std;

/***************** Logger *****************/

// create a static logger, local to the current function, named with
// the function's name
#define INIT_LOCAL_LOGGER() \
    static log4cxx::LoggerPtr _local_logger = g_logger->getLogger(__func__)

// log using the current function's local logger (created by
// INIT_LOCAL_LOGGER)
#define LOGDEBUG(x) LOG4CXX_DEBUG(_local_logger, x)
#define LOGINFO(x) LOG4CXX_INFO(_local_logger, x)

void *transfer_keys(void *arg);
void *fix_fingers(void *arg);
void *stabilizer(void *arg);

/***************** Data Structures *****************/

typedef struct _finger_file {
    string name;
    string data;
} finger_file;

vector<finger_entry> finger_table;
pthread_mutex_t finger_lock = PTHREAD_MUTEX_INITIALIZER;

map<int32_t, finger_file> file_table;
pthread_mutex_t file_lock = PTHREAD_MUTEX_INITIALIZER;

int mbits;
int my_id;
int my_port;
finger_entry predecessor;

int stabilizeWait;      // in seconds
int fixWait;            // in seconds

/***************** Prototypes *****************/

string get_ADD_FILE_result_as_string(const char *fname,
                                     const int32_t key,
                                     const int32_t nodeId);

string get_DEL_FILE_result_as_string(const char *fname,
                                     const int32_t key,
                                     const bool deleted,
                                     const int32_t nodeId);

string get_GET_FILE_result_as_string(const char *fname,
                                     const int32_t key,
                                     const bool found,
                                     const int32_t nodeId,
                                     const char *fdata);

string get_GET_TABLE_result_as_string(
    const vector<finger_entry>& finger_table,
    const uint32_t m,
    const uint32_t myid,
    const uint32_t idx_of_entry1,
    const std::map<int32_t, finger_file>& keys_table);
 
int get_key_from_filename(char *filename, int m);

/***************** Helpers *****************/

/**
 * check if 'one' is closer to 'reference' than 'two' is
 * in the reversed direction.
 */
bool is_reverse_closer(int reference, int one, int two) {
    if (reference - one < reference - two) {
        return true;
    }
    return false;
}

/**
 * check if 'test' is in between low and hi.
 * lowInc: lower bound is inclusive, otherwise exclusive.
 * hiInc: higher bound is inclusive, otherwise exclusive.
 */
bool is_in_range(int test, int low, int hi, bool lowInc, bool hiInc) {
    INIT_LOCAL_LOGGER();

    int twoToM = (int)pow(2, mbits);
    
    if (low > hi) {
        hi += twoToM;
    }

    if (lowInc == false) {
        low++;
    }
    if (hiInc == false) {
        hi--;
    }

    if (test >= low && test <= hi) {
        return true;
    }
    return false;
}

/***************** Server (copied over from generated files) *****************/

class MyServiceHandler : virtual public MyServiceIf {
public:
    MyServiceHandler() {
        // initialization
    }

    // return the predecessor node of this node.
    void get_predecessor(finger_entry& _return) {
        INIT_LOCAL_LOGGER();

        _return.id = predecessor.id;
        _return.port = predecessor.port;
    }

    // return the successor node of this node.
    void get_successor(finger_entry& _return) {
        INIT_LOCAL_LOGGER();

        pthread_mutex_lock(&finger_lock);
        _return.id = finger_table[0].id;
        _return.port = finger_table[0].port;
        pthread_mutex_unlock(&finger_lock);
    }

    // ask node n to find id's successor.
    void find_successor(finger_entry& _return, const int32_t id) {
        INIT_LOCAL_LOGGER();
       
        // get predecessor of id, store it in 'result'.
        finger_entry result;
        find_predecessor(result, id);

        if (result.id == id) {
            _return.id = result.id;
            _return.port = result.port;
            return;
        }

        // get successor of 'result', store it in 'result1'.
        finger_entry result1;
        if (result.id == my_id) {
            get_successor(result1);
        }
        else {
            assert(result.port > 0);
            boost::shared_ptr<TSocket> socket1(new TSocket("localhost", result.port));
            boost::shared_ptr<TTransport> transport1(new TBufferedTransport(socket1));
            boost::shared_ptr<TProtocol> protocol1(new TBinaryProtocol(transport1));
            
            MyServiceClient client1(protocol1);
            transport1->open();
            client1.get_successor(result1);
            transport1->close();
        }
       
        // return
        _return.id = result1.id;
        _return.port = result1.port;
    }
    
    // ask node n to find id's predecessor.
    void find_predecessor(finger_entry& _return, const int32_t id) {
        INIT_LOCAL_LOGGER();
        
        finger_entry n;
        n.id = my_id;
        n.port = my_port;
        
        // get successor of n'
        finger_entry result1;
        pthread_mutex_lock(&finger_lock);
        result1.id = finger_table[0].id;
        result1.port = finger_table[0].port;
        pthread_mutex_unlock(&finger_lock);

        while (is_in_range(id, n.id, result1.id, false, true) == false) {
            // if successor has not even been intialized, just return itself.
            if (n.id == result1.id) {
                break;
            }

            finger_entry n_try;

            if (n.id == my_id) {
                closest_preceding_finger(n_try, id);
            }
            else {
                assert(n.port > 0);
                boost::shared_ptr<TSocket> socket2(new TSocket("localhost", n.port));
                boost::shared_ptr<TTransport> transport2(new TBufferedTransport(socket2));
                boost::shared_ptr<TProtocol> protocol2(new TBinaryProtocol(transport2));
            
                MyServiceClient client2(protocol2);
                transport2->open();
                client2.closest_preceding_finger(n_try, id);
                transport2->close();
            }

            // if n' did not change, what n' is the closest preceding node.
            if (n.id == n_try.id) {
                break;
            }

            n.id = n_try.id;
            n.port = n_try.port;

            // n' just changed, get successor of n' again.
            assert(n.port > 0);
            boost::shared_ptr<TSocket> socket3(new TSocket("localhost", n.port));
            boost::shared_ptr<TTransport> transport3(new TBufferedTransport(socket3));
            boost::shared_ptr<TProtocol> protocol3(new TBinaryProtocol(transport3));
    
            MyServiceClient client3(protocol3);
            transport3->open();
            client3.get_successor(result1);
            transport3->close();
        }

        // return
        _return.id = n.id;
        _return.port = n.port;
    }

    // return closest finger preceding id.
    void closest_preceding_finger(finger_entry& _return, const int32_t id) {
        INIT_LOCAL_LOGGER();
        
        int i;
        for (i = mbits - 1; i >= 0; i--) {
            pthread_mutex_lock(&finger_lock);
            int nodeID = finger_table[i].id;
            pthread_mutex_unlock(&finger_lock);
            
            if (is_in_range(nodeID, my_id, id, false, false)) {
                pthread_mutex_lock(&finger_lock);
                _return.id = finger_table[i].id;
                _return.port = finger_table[i].port;
                pthread_mutex_unlock(&finger_lock);
                return;
            }
            // edge case for the ring.
            int twoToM = (int)pow(2, mbits);
            if (nodeID == 0 && my_id <= twoToM - 1 && id >= 1 && my_id > id) {
                pthread_mutex_lock(&finger_lock);
                _return.id = finger_table[i].id;
                _return.port = finger_table[i].port;
                pthread_mutex_unlock(&finger_lock);
                return;
            }
        }

        _return.id = my_id;
        _return.port = my_port;
    }

    // n thinks it might be my predecessor.
    void notify(const finger_entry& n) {
        INIT_LOCAL_LOGGER();

        if (n.id == my_id) {
            return;
        }

        // only change predecessor if we don't know any one closer.
        if ((predecessor.id == -1 || 
                is_reverse_closer(my_id, n.id, predecessor.id)) &&
                predecessor.id != n.id) {
            predecessor.id = n.id;
            predecessor.port = n.port;

            // logging
            stringstream stream;
            stream << "node= " << my_id << ": updated predecessor= " << predecessor.id;
            LOGINFO(stream.str());
            
            // start another thread for transfering keys
            int rc;
            pthread_t thread;
            rc = pthread_create(&thread, NULL, transfer_keys, NULL);
            if (rc) {
                exit(1);
            }
            pthread_detach(thread);
        }
    }

    // Add a new file to the hash table at this node.
    void gateway_add_file_self(std::string& _return, 
            const std::string& name, 
            const std::string& content) {
        INIT_LOCAL_LOGGER();
        
        string s;
        char *fileName = (char *)name.c_str();
        int key = get_key_from_filename(fileName, mbits);
    
        finger_file the_file;
        the_file.name = name;
        the_file.data = content;

        pthread_mutex_lock(&file_lock);
        file_table[key] = the_file;
        pthread_mutex_unlock(&file_lock);
        s = get_ADD_FILE_result_as_string(fileName,
                                          key, 
                                          my_id);
       
        // logging
        stringstream stream;
        stream << "node= " << my_id << ": added file: k= " << key;
        LOGINFO(stream.str());

        _return = s;
    }
        
    // Add a new file to the hash table at other node.
    void gateway_add_file_other(std::string& _return, 
            const std::string& name, 
            const std::string& content) {
        INIT_LOCAL_LOGGER();
        
        string s;
        char *fileName = (char *)name.c_str();
        int key = get_key_from_filename(fileName, mbits);
       
        finger_entry succ;
        find_successor(succ, key);
      
        if (succ.id == my_id) {
            gateway_add_file_self(s, name, content);
        }
        else {
            assert(succ.port > 0);
            boost::shared_ptr<TSocket> socket3(new TSocket("localhost", succ.port));
            boost::shared_ptr<TTransport> transport3(new TBufferedTransport(socket3));
            boost::shared_ptr<TProtocol> protocol3(new TBinaryProtocol(transport3));
    
            MyServiceClient client3(protocol3);
            transport3->open();
            client3.gateway_add_file_self(s, name, content);
            transport3->close();
        }
        
        _return = s;
    }

    // delete file at this node.    
    void gateway_del_file_self(std::string& _return, 
            const std::string& name) {
        INIT_LOCAL_LOGGER();
        
        string s;
        char *fileName = (char *)name.c_str();
        int key = get_key_from_filename(fileName, mbits);

        pthread_mutex_lock(&file_lock);
        if (file_table.find(key) != file_table.end()) {
            // exist
            file_table.erase(key);
            s = get_DEL_FILE_result_as_string(fileName,
                                              key,
                                              true,
                                              my_id);
        
            // logging
            stringstream stream;
            stream << "node= " << my_id << ": deleted file: k= " << key;
            LOGINFO(stream.str());
        }
        else {
            // not exist
            s = get_DEL_FILE_result_as_string(fileName,
                                              key,
                                              false,
                                              my_id);
        
            // logging
            stringstream stream;
            stream << "node= " << my_id << ": no such file k= " << key << " to delete";
            LOGINFO(stream.str());
        }
        pthread_mutex_unlock(&file_lock);

        _return = s;
    }

    // delete file at other node.
    void gateway_del_file_other(std::string& _return, 
            const std::string& name) {
        INIT_LOCAL_LOGGER();
        
        string s;
        char *fileName = (char *)name.c_str();
        int key = get_key_from_filename(fileName, mbits);
       
        finger_entry succ;
        find_successor(succ, key);
        
        if (succ.id == my_id) {
            gateway_del_file_self(s, name);
        }
        else {
            assert(succ.port > 0);
            boost::shared_ptr<TSocket> socket3(new TSocket("localhost", succ.port));
            boost::shared_ptr<TTransport> transport3(new TBufferedTransport(socket3));
            boost::shared_ptr<TProtocol> protocol3(new TBinaryProtocol(transport3));
    
            MyServiceClient client3(protocol3);
            transport3->open();
            client3.gateway_del_file_self(s, name);
            transport3->close();
        }
        
        _return = s;
    }

    // get file at this node.
    void gateway_get_file_self(std::string& _return, 
            const std::string& name) {
        INIT_LOCAL_LOGGER();
        
        string s;
        char *fileName = (char *)name.c_str();
        int key = get_key_from_filename(fileName, mbits);
 
        pthread_mutex_lock(&file_lock);
        if (file_table.find(key) != file_table.end()) {
            // exist
            const char *cont = file_table[key].data.c_str();
            s = get_GET_FILE_result_as_string(fileName,
                                              key,
                                              true,
                                              my_id,
                                              cont);

            // logging
            stringstream stream;
            stream << "node= " << my_id << ": served file: k= " << key;
            LOGINFO(stream.str());
        }
        else {
            // not exist
            s = get_GET_FILE_result_as_string(fileName,
                                              key,
                                              false,
                                              my_id, 
                                              NULL);

            // logging
            stringstream stream;
            stream << "node= " << my_id << ": no such file k= " << key << " to serve";
            LOGINFO(stream.str());
        }
        pthread_mutex_unlock(&file_lock);

        _return = s;
    }

    // get file at other node.
    void gateway_get_file_other(std::string& _return, 
            const std::string& name) {
        INIT_LOCAL_LOGGER();
        
        string s;
        char *fileName = (char *)name.c_str();
        int key = get_key_from_filename(fileName, mbits);
       
        finger_entry succ;
        find_successor(succ, key);
        
        if (succ.id == my_id) {
            gateway_get_file_self(s, name);
        }
        else {
            assert(succ.port > 0);
            boost::shared_ptr<TSocket> socket3(new TSocket("localhost", succ.port));
            boost::shared_ptr<TTransport> transport3(new TBufferedTransport(socket3));
            boost::shared_ptr<TProtocol> protocol3(new TBinaryProtocol(transport3));
    
            MyServiceClient client3(protocol3);
            transport3->open();
            client3.gateway_get_file_self(s, name);
            transport3->close();
        }
        
        _return = s;
    }

    // Return this node's table as string.
    void gateway_get_self_table(std::string& _return) {
        INIT_LOCAL_LOGGER();

        string result = get_GET_TABLE_result_as_string(
                finger_table, mbits, my_id, 0, file_table);
        _return = result;
    }

    // Return node 'id''s table as string.
    void gateway_get_other_table(std::string& _return, const int32_t id) {
        INIT_LOCAL_LOGGER();
        
        finger_entry result_entry;
        finger_entry pred;
        find_predecessor(pred, id);

        if (pred.id == id) {
            result_entry.id = pred.id;
            result_entry.port = pred.port;
        }
        else {
            if (pred.id == my_id) {
                get_successor(result_entry);
            }
            else {
                assert(pred.port > 0);
                boost::shared_ptr<TSocket> socket2(new TSocket("localhost", pred.port));
                boost::shared_ptr<TTransport> transport2(new TBufferedTransport(socket2));
                boost::shared_ptr<TProtocol> protocol2(new TBinaryProtocol(transport2));
            
                MyServiceClient client2(protocol2);
                transport2->open();
                client2.get_successor(result_entry);
                transport2->close();
            }
        }
        
        string result;
        if (result_entry.id == my_id) {
            gateway_get_self_table(result);
        }
        else {
            assert(result_entry.port > 0);
            boost::shared_ptr<TSocket> socket3(new TSocket("localhost", result_entry.port));
            boost::shared_ptr<TTransport> transport3(new TBufferedTransport(socket3));
            boost::shared_ptr<TProtocol> protocol3(new TBinaryProtocol(transport3));
        
            MyServiceClient client3(protocol3);
            transport3->open();
            client3.gateway_get_self_table(result);
            transport3->close();
        }

        _return = result;
    }
};

// join the ring by asking introducer.
// This function will only be called from the main function once.
void join(const finger_entry& introducer) {
    INIT_LOCAL_LOGGER();

    int i;
    pthread_mutex_lock(&finger_lock);
    for (i = 0; i < mbits; i++) {
        finger_table[i].id = my_id;
        finger_table[i].port = my_port;
    }
    pthread_mutex_unlock(&finger_lock);

    // set predecessor to nil.
    predecessor.id = -1;
    predecessor.port = -1;
    
    // ask introducer to find my successor.
    if (my_id != introducer.id) {
        assert(introducer.port > 0);
        boost::shared_ptr<TSocket> socket2(new TSocket("localhost", introducer.port));
        boost::shared_ptr<TTransport> transport2(new TBufferedTransport(socket2));
        boost::shared_ptr<TProtocol> protocol2(new TBinaryProtocol(transport2));
    
        MyServiceClient client2(protocol2);
        transport2->open();
        finger_entry result2;
        client2.find_successor(result2, my_id);
        transport2->close();
    
        pthread_mutex_lock(&finger_lock);
        finger_table[0].id = result2.id;
        finger_table[0].port = result2.port;
        pthread_mutex_unlock(&finger_lock);
    }

    // logging
    stringstream stream;
    stream << "node= " << my_id << ": initial successor= " << finger_table[0].id;
    LOGINFO(stream.str());

    int rc;
    pthread_t thread;
    
    // Start the threads for stabilizing and finger fixing.
    rc = pthread_create(&thread, NULL, fix_fingers, NULL);
    if (rc) {
        exit(1);
    }
    rc = pthread_create(&thread, NULL, stabilizer, NULL);
    if (rc) {
        exit(1);
    }
}

/***************** Outputs *****************/

/**
 * example output:
 * fname= foo.c
 * key= 3
 * added to node= 4
 */
string get_ADD_FILE_result_as_string(const char *fname,
                                     const int32_t key,
                                     const int32_t nodeId) {
    std::stringstream s;
    s << "fname= " << fname << "\n";
    s << "key= " << key << "\n";
    s << "added to node= " << nodeId << "\n";
    return s.str();
}

/**
 * example output:
 * fname= foo.c
 * key= 3
 * file not found
 *
 * example output:
 * fname= bar.h
 * key= 6
 * was stored at node= 0
 * deleted
 */
string get_DEL_FILE_result_as_string(const char *fname,
                                     const int32_t key,
                                     const bool deleted,
                                     const int32_t nodeId) {
    std::stringstream s;
    s << "fname= " << fname << "\n";
    s << "key= " << key << "\n";
    if (deleted) {
        // then nodeId is meaningful
        s << "was stored at node= " << nodeId << "\n";
        s << "deleted\n";
    }
    else {
        // assume that this means file was not found
        s << "file not found\n";
    }
    return s.str();
}

/**
 * example output:
 * fname= foo.c
 * key= 3
 * file not found
 *
 * example output:
 * fname= bar.h
 * key= 6
 * stored at node= 0
 * fdata= this is file bar.h
 */
string get_GET_FILE_result_as_string(const char *fname,
                                     const int32_t key,
                                     const bool found,
                                     const int32_t nodeId,
                                     const char *fdata) {
    std::stringstream s;
    s << "fname= " << fname << "\n";
    s << "key= " << key << "\n";
    if (found) {
        // then nodeId is meaningful
        s << "stored at node= " << nodeId << "\n";
        s << "fdata= " << fdata << "\n";
    }
    else {
        // assume that this means file was not found
        s << "file not found\n";
    }
    return s.str();
}

/*
 * use this get_finger_table_as_string() function. when asked for its
 * finger table, a node should respond with the string returned by
 * this function.
 * 
 * myid is the id of the node calling this function.
 */
std::string
get_finger_table_as_string(const std::vector<finger_entry>& table,
                           const uint32_t m,
                           const uint32_t myid,
                           const uint32_t idx_of_entry1) {
    std::stringstream s;
    assert(table.size() == (idx_of_entry1 + m));
    s << "finger table:\n";
    for (size_t i = 1; (i - 1 + idx_of_entry1) < table.size(); ++i) {
        using std::setw;
        s << "entry: i= " << setw(2) << i << ", interval=["
          << setw(4) << (myid + (int)pow(2, i-1)) % ((int)pow(2, m))
          << ",   "
          << setw(4) << (myid + (int)pow(2, i)) % ((int)pow(2, m))
          << "),   node= "
          << setw(4) << table.at(i - 1 + idx_of_entry1).id
          << "\n";
    }
    return s.str();
}

/*
 * use this get_keys_table_as_string() function. when asked for its
 * keys table, a node should respond with the string returned by this
 * function.
 *
 * table is a mapping between the key and the file hashed to that key.
 */
std::string
get_keys_table_as_string(const std::map<int32_t, finger_file>& table) {
    std::stringstream s;
    std::map<int32_t, finger_file>::const_iterator it = table.begin();
    /* std::map keeps the keys sorted, so our iteration will be in
     * ascending order of the keys
     */
    s << "keys table:\n";
    for (; it != table.end(); ++it) {
        using std::setw;
        /* assuming file names are <= 10 chars long */
        s << "entry: k= " << setw(4) << it->first
          << ",  fname= " << setw(10) << it->second.name
          << ",  fdata= " << it->second.data
          << "\n";
    }
    return s.str();
}

/**
 * example output (node has 2 files):
 * finger table:
 * entry: i= 1, interval=[ 5, 6), node= 0
 * entry: i= 2, interval=[ 6, 0), node= 0
 * entry: i= 3, interval=[ 0, 4), node= 0
 * keys table:
 * entry: k= 1, fname= 123.doc, fdata= this is file 123.doc data
 * entry: k= 3, fname= 123.txt, fdata= this is file 123.txt data
 *
 * example output (node has no file):
 * finger table:
 * entry: i= 1, interval=[ 1, 2), node= 4
 * entry: i= 2, interval=[ 2, 4), node= 4
 * entry: i= 3, interval=[ 4, 0), node= 4
 * keys table:
 */
string get_GET_TABLE_result_as_string(
    const vector<finger_entry>& finger_table,
    const uint32_t m,
    const uint32_t myid,
    const uint32_t idx_of_entry1,
    const std::map<int32_t, finger_file>& keys_table) {
    
    return get_finger_table_as_string(
           finger_table, m, myid, idx_of_entry1) \
           + \
           get_keys_table_as_string(keys_table);
}

/***************** Hashing *****************/

int get_key_from_filename(char *filename, int m) {
    INIT_LOCAL_LOGGER();

    SHA1Context sha;
    int key;

    SHA1Reset(&sha);
    SHA1Input(&sha, (unsigned char *)filename, strlen(filename));

    SHA1Result(&sha);
    key = sha.Message_Digest[4] % ((int)pow(2, m));

    return key;
}

/***************** Periodical protocols *****************/

/**
 * Called only when this node has a new predecessor.
 */
void *transfer_keys(void *arg) {
    INIT_LOCAL_LOGGER();

    // changed predecessor, might need to transfer some files to it.
    if (predecessor.id != my_id) {
        map<int32_t, finger_file>::iterator p1;

        for (p1 = file_table.begin(); p1 != file_table.end(); ) {
           if (p1->first <= predecessor.id) {
                // transfer the file to the new predecessor.
                assert(predecessor.port > 0);
                boost::shared_ptr<TSocket> socket3(new TSocket("localhost", predecessor.port));
                boost::shared_ptr<TTransport> transport3(new TBufferedTransport(socket3));
                boost::shared_ptr<TProtocol> protocol3(new TBinaryProtocol(transport3));
    
                MyServiceClient client3(protocol3);
                transport3->open();
                string s;       // s is ignored
                client3.gateway_add_file_self(s, 
                        p1->second.name,
                        p1->second.data);
                transport3->close();

                // erase local copy
                assert(my_port > 0);
                boost::shared_ptr<TSocket> socket1(new TSocket("localhost", my_port));
                boost::shared_ptr<TTransport> transport1(new TBufferedTransport(socket1));
                boost::shared_ptr<TProtocol> protocol1(new TBinaryProtocol(transport1));
    
                MyServiceClient client1(protocol1);
                transport1->open();
                client1.gateway_del_file_self(s, (p1++)->second.name);
                transport1->close();
           }
           else {
               ++p1;
           }
        }
    }

    return NULL;
}

/**
 * Periodically refresh finger table entries.
 */
void *fix_fingers(void *arg) {
    INIT_LOCAL_LOGGER();
    
    while (1) {
        // random index between 1 ~ (mbits - 1)
        int i = (rand() % (mbits - 1)) + 1;
        
        finger_entry fixed_entry;
        int start = my_id + (int)(pow(2, i));

        if (start >= (int)(pow(2, mbits))) {
            start = start % (int)(pow(2, mbits));
        }

        // get successor of 'start'.
        assert(my_port > 0);
        boost::shared_ptr<TSocket> socket2(new TSocket("localhost", my_port));
        boost::shared_ptr<TTransport> transport2(new TBufferedTransport(socket2));
        boost::shared_ptr<TProtocol> protocol2(new TBinaryProtocol(transport2));
   
        MyServiceClient client2(protocol2);
        transport2->open();
        client2.find_successor(fixed_entry, start);
        transport2->close();

        // fix the finger.
        pthread_mutex_lock(&finger_lock);
        if (finger_table[i].id != fixed_entry.id) {
            finger_table[i].id = fixed_entry.id;
            finger_table[i].port = fixed_entry.port;
        
            // logging
            stringstream stream;
            stream << "node= " << my_id << ": updated finger entry: i= " << (i + 1) 
                << ", pointer= " << finger_table[i].id;
            LOGINFO(stream.str());
        }
        pthread_mutex_unlock(&finger_lock);

        sleep(fixWait);
    }

    // should never reach here.
    return NULL;
}

/**
 * Periodically verify n's immediate succsor, and tell the 
 * successor about n.
 */
void *stabilizer(void *arg) {
    INIT_LOCAL_LOGGER();

    while (1) {
        finger_entry successor;
        pthread_mutex_lock(&finger_lock);
        successor.id = finger_table[0].id;
        successor.port = finger_table[0].port;
        pthread_mutex_unlock(&finger_lock);

        // get successor's predecessor.
        finger_entry succ_predecessor;
        assert(successor.port > 0);
        boost::shared_ptr<TSocket> socket3(new TSocket("localhost", successor.port));
        boost::shared_ptr<TTransport> transport3(new TBufferedTransport(socket3));
        boost::shared_ptr<TProtocol> protocol3(new TBinaryProtocol(transport3));
    
        MyServiceClient client3(protocol3);
        transport3->open();
        client3.get_predecessor(succ_predecessor);
        transport3->close();
       
        // check if the current successor is correct.
        pthread_mutex_lock(&finger_lock);
        if (succ_predecessor.id != -1 && 
                (finger_table[0].id == my_id || 
                is_in_range(succ_predecessor.id, my_id, finger_table[0].id, false, false)) &&
                finger_table[0].id != succ_predecessor.id) {
            finger_table[0].id = succ_predecessor.id;
            finger_table[0].port = succ_predecessor.port;
            
            // logging
            stringstream stream;
            stream << "node= " << my_id << ": updated finger entry: i= 1" 
                << ", pointer= " << finger_table[0].id;
            LOGINFO(stream.str());
        }
        pthread_mutex_unlock(&finger_lock);
        
        // tell the successor about n.
        finger_entry myself;
        myself.id = my_id;
        myself.port = my_port;
        
        assert(finger_table[0].port > 0);
        boost::shared_ptr<TSocket> socket1(new TSocket("localhost", finger_table[0].port));
        boost::shared_ptr<TTransport> transport1(new TBufferedTransport(socket1));
        boost::shared_ptr<TProtocol> protocol1(new TBinaryProtocol(transport1));
    
        MyServiceClient client1(protocol1);
        transport1->open();
        client1.notify(myself);
        transport1->close();
        
        sleep(stabilizeWait);
    }
    
    // should never reach here.
    return NULL;
}

/***************** Helpers *****************/

/***************** Main *****************/

#include <getopt.h>
#include <stdarg.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    INIT_LOCAL_LOGGER();
    int opt;
    int long_index;

    int m = -1;
    int id = -1;
    int port = -1;
    int introducerPort = -1;
    int stabilizeInterval = -1;
    int fixInterval = -1;
    int seed = -1;
    const char *logconffile = NULL;
    
    struct option long_options[] = {
        /* mandatory args */
        
        {"m", required_argument, 0, 1000},
        /* id of this node: 0 for introducer */
        {"id", required_argument, 0, 1001},
        /* port THIS node will listen on, at least for the
         * Chord-related API/service
         */
        {"port", required_argument, 0, 1002},

        /* optional args */

        /* if not introducer (id != 0), then this is required: port
         * the introducer is listening on.
         */
        {"introducerPort", required_argument, 0, 1003},
        /* path to the log configuration file */
        {"logConf", required_argument, 0, 1004},
        /* intervals (seconds) for runs of the stabilization and
         * fixfinger algorithms */
        {"stabilizeInterval", required_argument, 0, 1005},
        {"fixInterval", required_argument, 0, 1006},
        {"seed", required_argument, 0, 1007},
        {0, 0, 0, 0},
    };

    while ((opt = getopt_long(argc, argv, "", long_options, &long_index)) != -1) {
        switch (opt) {
        case 0:
            if (long_options[long_index].flag != 0) {
                break;
            }
            printf("option %s ", long_options[long_index].name);
            if (optarg) {
                printf("with arg %s\n", optarg);
            }
            printf("\n");
            break;
        case 1000:
            m = strtol(optarg, NULL, 10);
            assert((m >= 3) && (m <= 10));
            break;
        case 1001:
            id = strtol(optarg, NULL, 10);
            assert(id >= 0);
            break;
        case 1002:
            port = strtol(optarg, NULL, 10);
            assert(port > 0);
            break;
        case 1003:
            introducerPort = strtol(optarg, NULL, 10);
            assert(introducerPort > 0);
            break;
        case 1004:
            logconffile = optarg;
            break;
        case 1005:
            stabilizeInterval = strtol(optarg, NULL, 10);
            assert(stabilizeInterval > 0);
            break;
        case 1006:
            fixInterval = strtol(optarg, NULL, 10);
            assert(fixInterval > 0);
            break;
        case 1007:
            seed = strtol(optarg, NULL, 10);
            break;
        default:
            exit(1);
        }
    }

    assert((m >= 3) && (m <= 10));
    assert((id >= 0) && (id < pow(2, m)));
    assert(port > 0);
    if (seed != -1) {
        srand(seed);
    }
    configureLogging(logconffile);

    // Initialization.
    mbits = m;
    my_id = id;
    my_port = port;
    finger_table.resize(mbits);

    stabilizeWait = stabilizeInterval;
    fixWait = fixInterval;
 
    // If user didn't specify stabilize period/fix fingers period, initialize
    // them to the defaults.
    if (stabilizeWait == -1) {
        stabilizeWait = 1;
    }
    if (fixWait == -1) {
        fixWait = 1;
    }

    // Join the ring.
    finger_entry intro;
    if (my_id != 0) {
        assert(introducerPort > 0);

        intro.id = 0;
        intro.port = introducerPort;
    }
    else {
        intro.id = my_id;
        intro.port = my_port;
    }
    join(intro);
    
    // Start the server:
    shared_ptr<MyServiceHandler> handler(new MyServiceHandler());
    shared_ptr<TProcessor> processor(new MyServiceProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();

    return 0;
}

