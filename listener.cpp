#include <sstream>
#include <getopt.h>
#include <stdarg.h>
#include <assert.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <limits>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>

#include "MyService.h"
#include <transport/TSocket.h>
#include <transport/TBufferTransports.h>
#include <protocol/TBinaryProtocol.h>

using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace mp2;
using namespace std;

int *using_ports;
int ports_len;
int ports_total;

/**
 * Return a usable port.
 */
int get_usable_port(int startingPort) {
    int sockfd;
	struct addrinfo hints, *servinfo, *p;
	int yes = 1;
    int rv;

    memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE; // use my IP

    int portToTry = startingPort - 1;
    while (1) {
        if (startingPort != -1) {
            portToTry++;
        }
        else {
            // 1024 ~ 65535, pick a random one
            portToTry = (rand() % (65536 - 1024)) + 1024;
        }

        // first check to see if it is in the array.
        int i;
        bool okay = true;
        for (i = 0; i < ports_len; i++) {
            if (using_ports[i] == portToTry) {
                okay = false;
                break;
            }
        }
        if (okay == false) {
            continue;
        }

        char portBuffer[6];
        sprintf(portBuffer, "%d", portToTry);

	    if ((rv = getaddrinfo(NULL, portBuffer, &hints, &servinfo)) != 0) {
            continue;
        }

        // loop through all the results and bind to the first we can
	    for(p = servinfo; p != NULL; p = p->ai_next) {
		    if ((sockfd = socket(p->ai_family, p->ai_socktype,
				    p->ai_protocol)) == -1) {
			    continue;
		    }
		    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes,
				    sizeof(int)) == -1) {
		        continue;
            }
		    if (bind(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
			    close(sockfd);
			    continue;
		    }
		    break;
	    }
        if (p == NULL) {
	        continue;
        }
        freeaddrinfo(servinfo);
        
        if (listen(sockfd, 10) == -1) {
	        continue;
        }
        
        // here, we know it is usable, release it and return it.
        close(sockfd);
        
        // put it in array first.
        using_ports[ports_len] = portToTry;
        ports_len++;
        if (ports_len == ports_total) {
            ports_total *= 2;
            using_ports = (int *)realloc(using_ports, sizeof(int) * ports_total);
        }

        return portToTry;
    }
}

void add_new_node(int introducerPort, int m, int node, int startingPort, 
                        int stabilizeInterval, int fixInterval, const char *logconffile) {
    int port = get_usable_port(startingPort);

    pid_t pid = fork();
    if (pid == 0) {
        std::stringstream s;
        s << "./node --m " << m;
        s << " --id " << node;
        s << " --port " << port;
        s << " --introducerPort " << introducerPort;
        if (stabilizeInterval != -1) {
            s << " --stabilizeInterval " << stabilizeInterval;
        }
        if (fixInterval != -1) {
            s << " --fixInterval " << fixInterval;
        }
        if (logconffile != NULL) {
            s << " --logConf " << logconffile;
        }
        s << " --seed " << time(NULL);

        system(s.str().c_str());
        exit(0);
    }
}

int main(int argc, char **argv) {
    string command;
    int myNodePort;     // the port of the node this listener is listening on.

    int opt;
    int long_index;

    int startingPort = -1;
    int attachingNode = -1;
    int m = -1;
    int stabilizeInterval = -1;
    int fixInterval = -1;
    const char *logconffile = NULL;

    struct option long_options[] = {
        /* mandatory args */
        
        {"m", required_argument, 0, 1000},

        /* optional args */
        
        {"startingPort", required_argument, 0, 1001},
        {"attachToNode", required_argument, 0, 1002},
        {"stabilizeInterval", required_argument, 0, 1003},
        {"fixInterval", required_argument, 0, 1004},
        {"logConf", required_argument, 0, 1005},
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
            startingPort = strtol(optarg, NULL, 10);
            assert(startingPort > 0);
            break;
        case 1002:
            attachingNode = strtol(optarg, NULL, 10);
            assert(attachingNode >= 0);
            break;
        case 1003:
            stabilizeInterval = strtol(optarg, NULL, 10);
            assert(stabilizeInterval > 0);
            break;
        case 1004:
            fixInterval = strtol(optarg, NULL, 10);
            assert(fixInterval > 0);
            break;
        case 1005:
            logconffile = optarg;
            break;
        default:
            exit(1);
        }
    }

    assert((m >= 3) && (m <= 10));

    ports_len = 0;
    ports_total = 10;
    using_ports = (int *)malloc(sizeof(int) * ports_total);

    // This is only used if we launch the introducer.
    int usablePort = get_usable_port(startingPort);

    // If not attaching, start node 0.
    if (attachingNode == -1) {
        myNodePort = usablePort;

        pid_t pid = fork();
        if (pid == 0) {
            std::stringstream s;
            s << "./node --m " << m;
            s << " --id 0";
            s << " --port " << usablePort;
            if (stabilizeInterval != -1) {
                s << " --stabilizeInterval " << stabilizeInterval;
            }
            if (fixInterval != -1) {
                s << " --fixInterval " << fixInterval;
            }
            if (logconffile != NULL) {
                s << " --logConf " << logconffile;
            }
            s << " --seed " << time(NULL);
        
            system(s.str().c_str());
            exit(0);
        }
    }
    else {
        myNodePort = attachingNode;
    }

    // Start taking command.
    while (1) {
        string line;
        getline(cin, line);
        stringstream ss(line);
        
        string command;
        ss >> command;

        if (command.compare("ADD_NODE") == 0) {
            if (attachingNode != -1) {
                cout << "Only introducer can add node." << endl;
                cin.ignore(numeric_limits<streamsize>::max(), '\n');
                continue;
            }

            while (ss.good()) {
                int node;
                ss >> node;

                add_new_node(usablePort, m, node, startingPort, 
                        stabilizeInterval, fixInterval, logconffile);
            }
        }
        else if (command.compare("ADD_FILE") == 0) {
            string fileName;
            ss >> fileName;
            ss.ignore();
            string data;
            getline(ss, data);

            // call my node to get the result.
            boost::shared_ptr<TSocket> socket(new TSocket("localhost", myNodePort));
            boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
            boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

            MyServiceClient client(protocol);
            transport->open();
            std::string result;
            client.gateway_add_file_other(result, fileName, data);
            transport->close();
            
            // print out the result.
            cout << result;
        }
        else if (command.compare("DEL_FILE") == 0) {
            string fileName;
            ss >> fileName;
            
            // call my node to get the result.
            boost::shared_ptr<TSocket> socket(new TSocket("localhost", myNodePort));
            boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
            boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

            MyServiceClient client(protocol);
            transport->open();
            std::string result;
            client.gateway_del_file_other(result, fileName);
            transport->close();
            
            // print out the result.
            cout << result;
        }
        else if (command.compare("GET_FILE") == 0) {
            string fileName;
            ss >> fileName;

            // call my node to get the result.
            boost::shared_ptr<TSocket> socket(new TSocket("localhost", myNodePort));
            boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
            boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

            MyServiceClient client(protocol);
            transport->open();
            std::string result;
            client.gateway_get_file_other(result, fileName);
            transport->close();
            
            // print out the result.
            cout << result;
        }
        else if (command.compare("GET_TABLE") == 0) {
            int nodeID;
            ss >> nodeID;
            
            // call my node to get the result.
            boost::shared_ptr<TSocket> socket(new TSocket("localhost", myNodePort));
            boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
            boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));

            MyServiceClient client(protocol);
            transport->open();
            std::string result;
            client.gateway_get_other_table(result, nodeID);
            transport->close();

            // print out the result.
            cout << result;
        }
        else {
            cout << "Unrecognized command." << endl;
        }
    }
}

