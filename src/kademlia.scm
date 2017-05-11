(module dust.kademlia

;;;;; Exports ;;;;;
(make-node
 node-ip
 node-ip-set!
 node-port
 node-port-set!
 node-id
 node-id-set!
 node-first-seen
 node-first-seen-set!
 node-last-seen
 node-last-seen-set!
 node-failed-requests
 node-failed-requests-set!
 update-node
 node->blob
 blob->node
 kademlia-env-open
 store-open
 store-txn
 store-routing-table
 store-metadata
 routing-table-open
 metadata-open
 random-id
 local-id
 local-id-set!
 with-store
 prefix->blob
 blob->prefix
 prefix-blob-compare
 node-blob-compare
 max-bucket-size
 bucket-nodes
 bucket-insert
 bucket-remove
 bucket-destroy
 bucket-split
 bucket-join
 bucket-size
 find-bucket-for-id
 update-routing-table
 rpc-manager-start
 rpc-manager-listener
 rpc-manager-dispatcher
 rpc-manager-inbox
 rpc-manager-outbox
 rpc-manager-cancellations
 rpc-manager-threads-join
 rpc-manager-threads-terminate
 rpc-send
 send-ping
 send-store
 send-find-value
 send-find-node
 server-start
 server-stop
 with-server
 server-env
 server-id
 server-id-set!
 server-add-node
 server-all-nodes
 server-has-node?
 server-events
 join-network
 distance
 closest-node
 k-closest-nodes)

(import chicken scheme foreign)

(use lmdb-lolevel
     lazy-seq
     srfi-4
     srfi-1
     data-structures
     sodium
     posix
     extras
     udp6
     socket
     srfi-18
     srfi-69
     gochan
     bencode
     matchable
     bitstring
     defstruct
     log5scm
     dust.u8vector-utils
     dust.bitstring-utils
     dust.lazy-seq-utils
     dust.alist-match
     dust.lmdb-utils)

;; LMDB does not work RDONLY on OpenBSD and should be used in MDB_WRITEMAP mode
(define openbsd (string=? (car (system-information)) "OpenBSD"))
(define env-flags (if openbsd MDB_WRITEMAP 0))


;; logging category
(define-category kademlia)
(define-category debug)

(foreign-declare "#include <lmdb.h>")
(foreign-declare "#include <string.h>")
(foreign-declare "#include <stdint.h>")

(define max-bucket-size (make-parameter 20))

(defstruct node
  id
  ip
  port
  first-seen
  last-seen
  failed-requests)

(define-record-printer (node x out)
  (fprintf out "#<node ~S ~S ~S first:~S last:~S failed:~S>"
           (node-id x)
           (node-ip x)
           (node-port x)
           (node-first-seen x)
           (node-last-seen x)
           (node-failed-requests x)))

(define-record store
  txn
  routing-table
  metadata
  data)

(define-record-printer (store x out)
  (fprintf out "#<store ~S ~S ~S>"
           (store-txn x)
           (store-routing-table x)
           (store-metadata x)))

(define (node->blob node)
  (assert (= 20 (blob-size (node-id node))))
  (bitstring->blob
   (bitconstruct
    ((node-id node) 160 bitstring)
    ((node-first-seen node) 64 host signed)
    ((node-last-seen node) 64 host signed)
    ((node-failed-requests node) 8 host unsigned)
    ((node-port node) 16 host unsigned)
    ((string->bitstring (node-ip node)) bitstring))))

(define (blob->node blob)
  (bitmatch blob
    (((id 160 bitstring)
      (first-seen-secs 64 host signed)
      (last-seen-secs 64 host signed)
      (failed-requests 8 host unsigned)
      (port 16 host unsigned)
      (ip bitstring))
     (make-node id: (bitstring->blob id)
                ip: (bitstring->string ip)
                port: port
                first-seen: first-seen-secs
                last-seen: last-seen-secs
                failed-requests: failed-requests))))

(define (kademlia-env-open path)
  (create-directory path #t)
  (let ((env (mdb-env-create)))
    (mdb-env-set-mapsize env 10000000)
    (mdb-env-set-maxdbs env 3)
    (mdb-env-open env path (bitwise-ior env-flags MDB_NOSYNC)
                  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    env))

(define (store-open txn)
  (make-store txn
              (routing-table-open txn)
              (metadata-open txn)
              (data-open txn)))

(define (with-store env thunk)
  (with-transaction env (compose thunk store-open)))

(define (prefix->blob prefix)
  (bitstring->blob
   (bitconstruct
    ((bitstring-length prefix) 8 host unsigned)
    (prefix bitstring))
   'right))

(define (blob->prefix blob)
  (bitmatch blob
            (((len 8 host unsigned)
              (prefix bitstring))
             (bitstring-take prefix len))))

;; sorts bitstrings (which may not fully align with byte boundaries)
(foreign-declare
 "static unsigned char _kademlia_prefix_cmp_bit_masks[] = {
    0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01
  };

  int kademlia_prefix_cmp(const MDB_val *a, const MDB_val *b) {
    unsigned char a_len = 0;
    unsigned char b_len = 0;
    unsigned char shortest_len = 0;
    unsigned char *a_bytes;
    unsigned char *b_bytes;
    unsigned char a_bit, b_bit;
    int aligned_bits, aligned_bytes, end, i = 0;

    if (a->mv_size > 0) a_len = ((unsigned char *)a->mv_data)[0];
    if (b->mv_size > 0) b_len = ((unsigned char *)b->mv_data)[0];

    a_bytes = &((unsigned char *)a->mv_data)[1];
    b_bytes = &((unsigned char *)b->mv_data)[1];

    if (a_len < b_len) {
      shortest_len = a_len;
    } else {
      shortest_len = b_len;
    }

    aligned_bytes = shortest_len/8;
    if (aligned_bytes) {
      int r = memcmp(a_bytes, b_bytes, aligned_bytes);
      if (r != 0) {
        return r;
      }
    }

    aligned_bits = shortest_len % 8;
    while (i < aligned_bits) {
      a_bit = a_bytes[aligned_bytes] & _kademlia_prefix_cmp_bit_masks[i];
      b_bit = b_bytes[aligned_bytes] & _kademlia_prefix_cmp_bit_masks[i];
      if (a_bit == b_bit) {
        i++;
      } else if (a_bit) {
        return 1;
      } else {
        return -1;
      }
    }

    end = aligned_bytes * 8 + i;
    if (end == a_len && end == b_len) {
      return 0;
    } else if (end == a_len) {
      return -1;
    }
    return 1;
  }")

(define prefix-blob-compare
  (foreign-lambda* int
   ((scheme-object a)
    (scheme-object b))
   "MDB_val val_a, val_b;
    C_i_check_bytevector(a);
    C_i_check_bytevector(b);
    val_a.mv_data = C_data_pointer(a);
    val_a.mv_size = C_header_size(a);
    val_b.mv_data = C_data_pointer(b);
    val_b.mv_size = C_header_size(b);
    C_return(kademlia_prefix_cmp(&val_a, &val_b));"))

;; Scheme reference to the custom C comparator function
(define kademlia_prefix_cmp
  (foreign-value "&kademlia_prefix_cmp"
                 (function int ((const (c-pointer (struct MDB_val)))
                                (const (c-pointer (struct MDB_val)))))))

;; sorts nodes stored in a dupsort db by first-seen timestamp field
(foreign-declare
 "int kademlia_node_cmp(const MDB_val *a, const MDB_val *b) {
    return memcmp(a->mv_data, b->mv_data, 20);
  }")

(define node-blob-compare
  (foreign-lambda* int
   ((scheme-object a)
    (scheme-object b))
   "MDB_val val_a, val_b;
    C_i_check_bytevector(a);
    C_i_check_bytevector(b);
    val_a.mv_data = C_data_pointer(a);
    val_a.mv_size = C_header_size(a);
    val_b.mv_data = C_data_pointer(b);
    val_b.mv_size = C_header_size(b);
    C_return(kademlia_node_cmp(&val_a, &val_b));"))

;; Scheme reference to the custom C comparator function
(define kademlia_node_cmp
  (foreign-value "&kademlia_node_cmp"
                 (function int ((const (c-pointer (struct MDB_val)))
                                (const (c-pointer (struct MDB_val)))))))

;; Opens routing table dbi with appropriate comparator and other options
(define (routing-table-open txn)
  (let ((dbi (mdb-dbi-open txn
                           "routing-table"
                           (bitwise-ior MDB_DUPSORT MDB_CREATE))))
    (mdb-set-compare txn dbi kademlia_prefix_cmp)
    (mdb-set-dupsort txn dbi kademlia_node_cmp)
    dbi))

(define (metadata-open txn)
  (mdb-dbi-open txn "metadata" MDB_CREATE))

(define (data-open txn)
  (mdb-dbi-open txn "data" MDB_CREATE))

(define (local-id-set! store id)
  (assert (= 20 (blob-size id)))
  (mdb-put (store-txn store)
           (store-metadata store)
           (string->blob "local-id")
           id
           0))

(define (random-id)
  (random-blob 20))

(define (local-id store)
  (condition-case
      (mdb-get (store-txn store)
               (store-metadata store)
               (string->blob "local-id"))
    ((exn lmdb MDB_NOTFOUND)
     (let ((new-id  (random-id)))
       (local-id-set! store new-id)
       new-id))))

(define (bucket-nodes store prefix)
  ;; (printf "bucket-nodes: ~S ~S ~S~n" txn dbi prefix)
  (lazy-map blob->node (dup-values (store-txn store)
                                   (store-routing-table store)
                                   (prefix->blob prefix))))

(define (bucket-insert store prefix node)
  ;; (printf "bucket-insert: ~S ~S ~S ~S~n" txn dbi prefix node)
  (mdb-put (store-txn store)
           (store-routing-table store)
           (prefix->blob prefix)
           (node->blob node)
           0))

(define (bucket-remove store prefix node)
  ;; (printf "bucket-remove: ~S ~S ~S ~S~n" txn dbi prefix node)
  (mdb-del (store-txn store)
           (store-routing-table store)
           (prefix->blob prefix)
           (node->blob node)))

(define (bucket-destroy store prefix)
  ;; (printf "bucket-destroy: ~S ~S ~S~n" txn dbi prefix)
  (mdb-del (store-txn store)
           (store-routing-table store)
           (prefix->blob prefix)))

(define (bucket-split store prefix)
  ;; (printf "bucket-split: ~S ~S~n" store prefix)
  (let ((upper (bitstring-append prefix (list->bitstring '(1))))
        (lower (bitstring-append prefix (list->bitstring '(0)))))
    (lazy-each (lambda (node)
                 (if (bitstring-bit-set?
                      (blob->bitstring (node-id node))
                      (bitstring-length prefix))
                     (bucket-insert store upper node)
                     (bucket-insert store lower node)))
               (bucket-nodes store prefix))
    (bucket-destroy store prefix)))

(define (bucket-join store parent)
  ;; (printf "bucket-join: ~S ~S ~S~n" txn dbi parent)
  (let* ((child-0 (bitstring-append parent (list->bitstring '(0))))
         (child-1 (bitstring-append parent (list->bitstring '(1))))
         (parent-key (prefix->blob parent))
         (child-0-key (prefix->blob child-0))
         (child-1-key (prefix->blob child-1))
         (txn (store-txn store))
         (dbi (store-routing-table store)))
    (lazy-each (cut mdb-put txn dbi parent-key <> 0)
               (dup-values txn dbi child-0-key))
    (lazy-each (cut mdb-put txn dbi parent-key <> 0)
               (dup-values txn dbi child-1-key))
    (mdb-del txn dbi child-0-key)
    (mdb-del txn dbi child-1-key)))

(define (bucket-size store prefix)
  (dup-count (store-txn store)
             (store-routing-table store)
             (prefix->blob prefix)))

(define (bucket-find store prefix id)
  (lazy-find
   (lambda (node)
     (blob=? (node-id node) id))
   (bucket-nodes store prefix)))

(define (find-bucket-for-id store id)
  (let ((id (blob->bitstring id)))
    (with-cursor
     (store-txn store)
     (store-routing-table store)
     (lambda (cursor)
       (let ((key (and (mdb-cursor-get/default
                        cursor (prefix->blob id) #f MDB_SET_RANGE #f)
                       (blob->prefix (mdb-cursor-key cursor)))))
         (cond
          ((not key)
           ;; reached end of keys, check if last key is prefix
           (and-let* ((last-key
                       (and (mdb-cursor-get/default cursor #f #f MDB_LAST #f)
                            (blob->prefix (mdb-cursor-key cursor)))))
             (and (bitstring-prefix? last-key id)
                  last-key)))
          ((bitstring-prefix? key id)
           ;; found a prefix
           key)
          (else
           ;; check if previous key is prefix
           (and-let* ((prev-key
                       (and (mdb-cursor-get/default cursor #f #f MDB_PREV #f)
                            (blob->prefix (mdb-cursor-key cursor)))))
             (and (bitstring-prefix? prev-key id)
                  prev-key)))))))))

(define (update-routing-table store id ip port)
  (let ((prefix (find-bucket-for-id store id)))
    (cond
     ;; if no bucket found, insert node into new bucket
     ((not prefix)
      (bucket-insert store
                     (bitstring-take (blob->bitstring id) 1)
                     (make-node id: id
                                ip: ip
                                port: port
                                first-seen: (current-seconds)
                                last-seen: (current-seconds)
                                failed-requests: 0)))
     ;; if node already exists, update last-seen
     ((bucket-find store prefix id) =>
      (lambda (existing)
        (bucket-insert store
                       prefix
                       (update-node existing
                                    last-seen: (current-seconds)))))
     ;; if bucket is full, split and try again or discard new ndoe
     ((>= (bucket-size store prefix) (max-bucket-size))
      ;; can bucket be split?
      (when (bitstring-prefix? prefix (blob->bitstring (local-id store)))
        (bucket-split store prefix)
        (update-routing-table store id ip port)))
     ;; bucket has space, insert new node
     (else
      (bucket-insert store
                     prefix
                     (make-node id: id
                                ip: ip
                                port: port
                                first-seen: (current-seconds)
                                last-seen: (current-seconds)
                                failed-requests: 0))))))

(define (receive-ping env tid params from-host from-port)
  (with-store env
              (lambda (store)
                `((tid . ,tid)
                  (type . "r")
                  (data . ,(blob->string (local-id store)))))))

(define (receive-store env tid params from-host from-port)
  (alist-match
   params
   ((key val)
    (if (and (string? key) (string? val))
        (begin
          (with-store env
                      (lambda (store)
                        (mdb-put (store-txn store)
                                 (store-data store)
                                 (string->blob key)
                                 (string->blob val)
                                 0)))
          (with-store env
                      (lambda (store)
                        `((tid . ,tid)
                          (type . "r")
                          (data . ,(blob->string (local-id store)))))))
        `((tid . ,tid)
          (type . "e")
          (code . 400)
          (desc . "Invalid STORE request"))))
   (else
    `((tid . ,tid)
      (type . "e")
      (code . 400)
      (desc . "Invalid STORE request")))))

(define (receive-find-value env tid params from-host from-port)
  (alist-match
   params
   ((key)
    (with-store
     env
     (lambda (store)
       (if (string? key)
           `((tid . ,tid)
             (type . "r")
             (data . ,(blob->string
                       (mdb-get (store-txn store)
                                (store-data store)
                                (string->blob key)))))
           `((tid . ,tid)
             (type . "e")
             (code . 400)
             (desc . "Invalid FIND_VALUE request"))))))
   (else
    `((tid . ,tid)
      (type . "e")
      (code . 400)
      (desc . "Invalid FIND_VALUE request")))))

(define (prev-closest-bucket prefix)
  (and (> (bitstring-length prefix) 0)
       (bitstring-drop-right prefix 1)))

(define (invert-last-bit b)
  (let ((prefix (bitstring-append (bitstring-drop-right b 1))))
    (if (bitstring-bit-set? b (- (bitstring-length b) 1))
        (bitconstruct (prefix bitstring) (0 1))
        (bitconstruct (prefix bitstring) (1 1)))))

(define (take-closest nodes n target-id)
  ;; (printf "take-closest: ~S ~S~n" n target-id)
  (map cdr
       (take
        (sort (map (lambda (node)
                     (cons (distance (node-id node) target-id)
                           node))
                   nodes)
              (lambda (a b) (u8vector<? (car a) (car b))))
        (min n (length nodes)))))

(define (take-at-most lst max-length)
  (cond ((= max-length 0) '())
        ((null? lst) '())
        (else
         (cons (car lst)
               (take-at-most (cdr lst) (- max-length 1))))))

(define (insert-sorted-max lst less? item max-length)
  ;; (printf "insert-sorted-max: ~S ~S ~S ~S~n" lst less? item max-length)
  (cond ((= max-length 0) '())
        ((null? lst) (list item))
        ((less? item (car lst))
         (cons item (take-at-most lst (- max-length 1))))
        (else
         (cons (car lst)
               (insert-sorted-max (cdr lst)
                                  less?
                                  item
                                  (- max-length 1))))))

(define (k-closest-nodes store id)
   (map cdr
        (lazy-fold
         (lambda (node results)
           ;; (printf "fold: ~S ~S~n" node results)
           (insert-sorted-max
            results
            (lambda (a b)
              (u8vector<? (car a) (car b)))
            (cons (distance (node-id node) id) node)
            (max-bucket-size)))
         '()
         (lazy-map blob->node
                   (all-values (store-txn store)
                               (store-routing-table store))))))

(define (receive-find-node env tid params from-host from-port)
  (alist-match
   params
   ((id)
    (with-store
     env
     (lambda (store)
       `((tid . ,tid)
         (type . "r")
         (data . ,(list->vector
                   (map (lambda (node)
                          (vector (blob->string (node-id node))
                                  (node-ip node)
                                  (node-port node)))
                        (k-closest-nodes store (string->blob id)))))))))
   (else
    `((tid . ,tid)
      (type . "e")
      (code . 400)
      (desc . "Invaliid FIND_NODE request")))))

(define rpc-handlers
  (alist->hash-table
   `(("PING" . ,receive-ping)
     ("STORE" . ,receive-store)
     ("FIND_VALUE" . ,receive-find-value)
     ("FIND_NODE" . ,receive-find-node))
   string=?
   string-hash))

;; places incoming bencoded messages onto a channel
(define (rpc-listener socket channel)
  (receive (n data from-host from-port) (udp-recvfrom socket 1200)
    (let ((msg (condition-case (string->bencode data)
                 ((exn bencode) #f))))
      (if msg
          (gochan-send channel (list msg from-host from-port))
          (begin
            (log-for (error kademlia)
                     "Invalid incoming bencode message (from ~A:~A)~n"
                     from-host
                     from-port)
            (log-for (debug kademlia) "~S" data)))))
  (rpc-listener socket channel))

;; TODO: validate that returned tid is from the same host:port that the original query was sent to!
(define (send-to-channel channels tid data from-host from-port)
  (let ((channel (hash-table-ref/default channels tid #f)))
    (if channel
        (begin
          (hash-table-delete! channels tid)
          (gochan-send channel data))
        (log-for (error kademlia)
                 "Missing transaction ID: ~A (from ~A:~A)~n"
                 (bin->hex (string->blob tid))
                 from-host
                 from-port))))

(define (rpc-dispatcher env socket inbox outbox cancellations waiting)
  (gochan-select
   ((inbox -> msg)
    (match-let (((data from-host from-port) msg))
      (log-for (debug kademlia)
               "received from ~A:~A ~S" from-host from-port data)
      (alist-match data
       (((type . "q") tid method)
        (let ((params (alist-ref 'params data))
              (handler (hash-table-ref/default rpc-handlers method #f)))
          (if handler
              (let ((data (handler env tid params from-host from-port)))
                (log-for (debug kademlia)
                         "sending to ~A:~A ~S"
                         from-host
                         from-port
                         data)
                (condition-case
                    (udp-sendto socket
                                from-host
                                from-port
                                (bencode->string data))
                  ((exn bencode)
                   (log-for (error kademlia)
                            "Invalid bencode returned by '~A' method handler~n"
                            method))))
              (log-for (error kademlia)
                       "No handler for method '~A' (from ~A:~A)~n"
                       method
                       from-host
                       from-port))))
       (((type . "r") tid)
        (send-to-channel waiting tid data from-host from-port))
       (((type . "e") tid)
        (send-to-channel waiting tid data from-host from-port))
       (else
        (log-for (error kademlia)
                 "Invalid message format (from ~A:~A)~n"
                 from-host
                 from-port)))))
   ((outbox -> msg)
    (match-let (((data to-host to-port channel) msg))
      (alist-match
       data
       ((tid)
        (hash-table-set! waiting tid channel)
        (udp-sendto socket to-host to-port (bencode->string data))))))
   ((cancellations -> tid)
    (hash-table-delete! waiting tid)))
  (rpc-dispatcher env socket inbox outbox cancellations waiting))

(define-record rpc-manager
  inbox outbox cancellations waiting listener dispatcher)

;; creates a new RPC manager for use with RPC calls, sets up
;; inbox/outbox channels and starts listener/dispatcher threads
(define (rpc-manager-start env socket)
  (let ((inbox (gochan 0))
        (outbox (gochan 0))
        (cancellations (gochan 0))
        (waiting (make-hash-table string=? string-hash)))
    (make-rpc-manager
     inbox
     outbox
     cancellations
     waiting
     (go (parameterize
             ((socket-receive-timeout #f))
           (rpc-listener socket inbox)))
     (go (rpc-dispatcher env socket inbox outbox cancellations waiting)))))

;; waits for all dispatcher threads to terminate
(define (rpc-manager-threads-join rpc-manager)
  (thread-join! (rpc-manager-listener rpc-manager))
  (thread-join! (rpc-manager-dispatcher rpc-manager)))

(define (rpc-manager-threads-terminate rpc-manager)
  (thread-terminate! (rpc-manager-listener rpc-manager))
  (thread-terminate! (rpc-manager-dispatcher rpc-manager)))


(define-record server env socket rpc-manager events)

(define-record-printer (server x out)
  (fprintf out "#<server ~A>" (bin->hex (server-id x))))

(define (server-start path host port)
  (let ((socket (udp-open-socket)))
    (udp-bind! socket host port)
    (let* ((env (kademlia-env-open path))
           (rpc-manager (rpc-manager-start env socket)))
      (make-server env socket rpc-manager (gochan 0)))))

(define (server-stop server)
  (rpc-manager-threads-terminate (server-rpc-manager server))
  (udp-close-socket (server-socket server))
  (mdb-env-close (server-env server)))

(define (with-server path host port thunk)
  (let ((server (server-start path host port)))
    (handle-exceptions exn
      (begin
        (server-stop server)
        (abort exn))
      (thunk server)
      (server-stop server))))

(define (server-id server)
  (with-store (server-env server)
              (cut local-id <>)))

(define (server-id-set! server id)
  ;; TODO: purge existing routing table, it is no longer valid!
  (with-store (server-env server)
              (cut local-id-set! <> id)))

(define (server-add-node server id ip port)
  (with-store (server-env server)
              (cut update-routing-table <> id ip port)))

(define (server-all-nodes server)
  (with-store (server-env server)
              (lambda (store)
                (lazy-seq->list
                 (lazy-map blob->node
                           (all-values (store-txn store)
                                       (store-routing-table store)))))))

(define (server-has-node? server)
  (with-store (server-env server)
              (lambda (store)
                (not (lazy-null?
                      (all-values (store-txn store)
                                  (store-routing-table store)))))))

(define (join-network server)
  (assert (server-has-node? server))
  (log-for (debug kademlia) "~S" `("join-start" ,(server-id server)))
  (find-node server (server-id server))
  (log-for (debug kademlia) "~S" `("join-complete" ,(server-id server))))

(define c-distance
  (foreign-lambda* int
      ((scheme-object a)
       (scheme-object b)
       (scheme-object result))
    "C_i_check_bytevector(a);
     C_i_check_bytevector(b);
     C_i_check_bytevector(result);
     if (C_header_size(a) != C_header_size(b) ||
         C_header_size(a) != C_header_size(result)) {
       C_return(1);
     }
     int len = C_header_size(a);
     int i;
     char *a_str = (char *)C_data_pointer(a);
     char *b_str = (char *)C_data_pointer(b);
     char *result_str = (char *)C_data_pointer(result);
     for (i = 0; i < len; i++) {
       result_str[i] = a_str[i] ^ b_str[i];
     }
     C_return(0);"))
     
(define (distance a b)
  (let ((result (make-blob (blob-size a))))
    (assert (= 0 (c-distance a b result)))
    (blob->u8vector result)))

(define (closest-node store id)
  (let* ((bucket (find-bucket-for-id store id))
         (nodes (bucket-nodes store bucket)))
    (lazy-fold (lambda (x closest)
                 (if (u8vector<? (distance x id) (distance closest id))
                     x
                     closest))
               (lazy-head nodes)
               (lazy-tail nodes))))

(define (find-node server target-id)
  (let ((node (with-store (server-env server)
                          (lambda (store)
                            (closest-node store target-id)))))
    (if (blob=? (node-id node) target-id)
        node
        (send-find-node server
                        (node-ip node)
                        (node-port node)
                        target-id))))

(define (rpc-send server to-host to-port method params #!key (timeout 5000))
  (let* ((manager (server-rpc-manager server))
         (tid (blob->string (random-id)))
         (response (gochan 0))
         (msg (append `((tid . ,tid)
                        (type . "q")
                        (method . ,method))
                      (if params `((params . ,params)) '()))))
    (gochan-send (rpc-manager-outbox manager)
                 (list msg to-host to-port response))
    (gochan-select
     ((response -> msg)
      (alist-match msg
                   (((type . "r") data) data)
                   (((type . "e") code desc)
                    (abort (make-composite-condition
                            (make-property-condition 'exn
                                                     'description desc)
                            (make-property-condition 'kademlia-rpc
                                                     'code code))))))
     (((gochan-after timeout) -> _)
      (gochan-send (rpc-manager-cancellations manager) tid)
      (abort (make-composite-condition
              (make-property-condition 'exn
                                       'description (string-append
                                                     method " call timed out"))
              (make-property-condition 'kademlia-rpc)
              (make-property-condition 'timeout)))))))
  
(define (send-ping server host port)
  (string->blob (rpc-send server host port "PING" #f)))

(define (send-store server host port key value)
  (rpc-send server host port "STORE" `((key . ,key) (val . ,value))))

(define (send-find-value server host port key)
  (rpc-send server host port "FIND_VALUE" `((key . ,key))))

(define (send-find-node server host port id)
  (map (lambda (row)
         (list (string->blob (vector-ref row 0))
               (vector-ref row 1)
               (vector-ref row 2)))
       (vector->list
        (rpc-send server host port "FIND_NODE" `((id . ,(blob->string id)))))))

)
