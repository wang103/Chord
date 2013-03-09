namespace cpp mp2

struct finger_entry {
    1: i32 id;
    2: i32 port;
}

/* declare the RPC interface of your network service */
service MyService {
    
    /* ask node n's predecessor */
    finger_entry get_predecessor();

    /* ask node n's successor */
    finger_entry get_successor();

    /* Figure 4 */
    
    /* ask node n to find id's successor */
    finger_entry find_successor(1:i32 id);

    /* ask node n to find id's predecessor */
    finger_entry find_predecessor(1:i32 id);

    /* return closest finger preceding id */
    finger_entry closest_preceding_finger(1:i32 id);

    /* Figure 7 */

    /* possible predecessor */
    void notify(1:finger_entry n);

    /* Gateways connecting to listener */

    string gateway_add_file_self(1:string name, 2:string content);
    string gateway_add_file_other(1:string name, 2:string content);
    
    string gateway_del_file_self(1:string name);
    string gateway_del_file_other(1:string name);

    string gateway_get_file_self(1:string name);
    string gateway_get_file_other(1:string name);

    string gateway_get_self_table();
    string gateway_get_other_table(1:i32 id);
}

