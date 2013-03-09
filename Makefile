GEN_SRC := MyService.cpp mp2_constants.cpp mp2_types.cpp
GEN_OBJ := $(patsubst %.cpp,%.o, $(GEN_SRC))
CXX += -DHAVE_NETINET_IN_H -g
THRIFT_DIR := /class/ece428/libs/include/thrift
BOOST_DIR := /usr/local/include

LOG4CXX_DIR := /class/ece428/libs/include

LINK += -L/class/ece428/libs/lib -Wl,-rpath,/class/ece428/libs/lib

INC += -I$(THRIFT_DIR) -I$(BOOST_DIR) -I$(LOG4CXX_DIR) -I/class/ece428/mp2

.PHONY: all clean

all: node listener

%.o: %.cpp
	$(CXX) -Wall $(INC) -c $< -o $@

node: node.o $(GEN_OBJ) /class/ece428/mp2/log.cpp /class/ece428/mp2/sha1.c
	$(CXX) $(INC) $(LINK) -llog4cxx -lthrift $^ -o $@

listener: listener.o $(GEN_OBJ)
	$(CXX) $(INC) $(LINK) -lthrift $^ -o $@

clean:
	$(RM) *.o node listener

