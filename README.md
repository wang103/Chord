CS 425 -- MP2
Tianyi Wang

This project implements a distributed hash table as presented in chord.pdf.

The two main source code files are listener.cpp and node.cpp.
(we used thrift for this MP, however we copied generated server code
into node.cpp, so node.cpp is a server and a client)

To compile everything, simply type 'make'.

The main output binary files are listener and node.
You can run them by typing ./listener or ./node with required arguments.

===================================================

Listener (listener.cpp):
Takes input from the user, and call corresponding methods in node.cpp.
node.cpp will handle the operation, and when it is done, it will return 
a result to listener. At last, listener will print out the result to the
terminal.

Node (node.cpp):
node.cpp keeps two data structures: a vector of fingers (finger table), and 
a map of all its key/file pairs.
In addition, each node also keep track of its predecessor.

When a node just launches, we launch two extra threads, one is for fixing 
finger table, it is called "fix_fingers()". Another is for fixing successor 
and transfering key/file, it is called "stabilizer()".

These two threads run periodically, if the user did not specify an interval,
we set the period to be 1 seconds.

When a node changes its predecessor, we transfer some of the node's key/file
to its new predecessor. The code for transfering key/file is in "transfer_keys()".

Everything else is exactly like the algorithms in chord.pdf, so currency and other
issues are handled correctly.
