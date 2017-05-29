(module dust.kademlia

;; TODO: make node record type and routing-table-entry record type (which includes last-seen etc)? replace all locations using 3-element list for node data with proper node record

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
 concurrency
 bucket-nodes
 bucket-insert
 bucket-remove
 bucket-destroy
 bucket-split
 bucket-join
 bucket-size
 find-bucket-for-id
 update-routing-table
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
 server-join-network
 distance
 n-closest-nodes
 server-find-node)

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
     srfi-69
     gochan
     bencode
     matchable
     bitstring
     defstruct
     log5scm
     srfi-18
     dust.u8vector-utils
     dust.bitstring-utils
     dust.lazy-seq-utils
     dust.alist-match
     dust.lmdb-utils
     dust.thread-monitor
     ;; dust.thread-pool
     dust.rpc-manager)

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
(define concurrency (make-parameter 3))

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
  (with-write-transaction env (compose thunk store-open)))

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
  ;; (printf "~S update-routing-table: ~S ~S ~S~n" (local-id store) id ip port)
  (unless (blob=? id (local-id store))
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
                                  failed-requests: 0)))))))

(define ((receive-ping env) tid params sid from-host from-port)
  (when sid
    (with-store env (cut update-routing-table <> sid from-host from-port)))
  (with-store env
              (lambda (store)
                `((tid . ,tid)
                  (type . "r")
                  (data . ,(blob->string (local-id store)))))))

(define ((receive-store env) tid params sid from-host from-port)
  (when sid
    (with-store env (cut update-routing-table <> sid from-host from-port)))
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

(define ((receive-find-value env) tid params sid from-host from-port)
  (when sid
    (with-store env (cut update-routing-table <> sid from-host from-port)))
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

(define (take-sorted-unique xs n #!optional (results '()))
  (cond ((or (= n 0) (null? xs))
         (reverse results))
        ((null? results)
         (take-sorted-unique (cdr xs) (- n 1) (list (car xs))))
        ((equal? (car results) (car xs))
         (take-sorted-unique (cdr xs) n results))
        (else
         (take-sorted-unique (cdr xs) (- n 1) (cons (car xs) results)))))

(define (take-closest-unique nodes n target-id)
  (map cdr
       (take-sorted-unique
        (sort (map (lambda (node)
                     (cons (distance (node-id node) target-id)
                           node))
                   nodes)
              (lambda (a b)
                (u8vector<? (car a) (car b))))
        n)))

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

(define (n-closest-nodes store id n)
   (map cdr
        (lazy-fold
         (lambda (node results)
           ;; (printf "fold: ~S ~S~n" node results)
           (insert-sorted-max
            results
            (lambda (a b)
              (u8vector<? (car a) (car b)))
            (cons (distance (node-id node) id) node)
            n))
         '()
         (lazy-map blob->node
                   (all-values (store-txn store)
                               (store-routing-table store))))))

(define ((receive-find-node env) tid params sid from-host from-port)
  (when sid
    (with-store env (cut update-routing-table <> sid from-host from-port)))
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
                        (n-closest-nodes store
                                         (string->blob id)
                                         (max-bucket-size)))))))))
   (else
    `((tid . ,tid)
      (type . "e")
      (code . 400)
      (desc . "Invaliid FIND_NODE request")))))

(define (rpc-handlers env)
  (alist->hash-table
   `(("PING" . ,(receive-ping env))
     ("STORE" . ,(receive-store env))
     ("FIND_VALUE" . ,(receive-find-value env))
     ("FIND_NODE" . ,(receive-find-node env)))
   string=?
   string-hash))

(define-record server env socket rpc-manager events)

(define-record-printer (server x out)
  (fprintf out "#<server ~A>" (bin->hex (server-id x))))

(define (server-start path host port)
  ;; (log-for (debug kademlia) "server-start: ~S ~S ~S" path host port)
  (let ((socket (udp-open-socket)))
    (udp-bind! socket host port)
    (let* ((env (kademlia-env-open path))
           (rpc-manager (rpc-manager-start socket (rpc-handlers env))))
      (make-server env socket rpc-manager (gochan 0)))))

(define (server-stop server)
  ;; (log-for (debug kademlia) "server-stop: ~S" server)
  (rpc-manager-stop (server-rpc-manager server))
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

(define (send-ping server host port)
  (string->blob
   (rpc-send (server-rpc-manager server)
             host
             port
             "PING"
             #f
             server-id: (server-id server))))

(define (send-store server host port key value)
  (rpc-send (server-rpc-manager server)
            host
            port
            "STORE"
            `((key . ,key) (val . ,value))
            server-id: (server-id server)))

(define (send-find-value server host port key)
  (rpc-send (server-rpc-manager server)
            host
            port
            "FIND_VALUE"
            `((key . ,key))
            server-id: (server-id server)))

(define (send-find-node server host port id)
  (let ((results
         (map (lambda (row)
                (list (string->blob (vector-ref row 0))
                      (vector-ref row 1)
                      (vector-ref row 2)))
              (vector->list
               (rpc-send (server-rpc-manager server)
                         host
                         port
                         "FIND_NODE"
                         `((id . ,(blob->string id)))
                         server-id: (server-id server))))))
    ;; (printf "find-node results: ~S~n" results)
    (for-each
     (lambda (node)
       (with-store (server-env server)
                   (lambda (store)
                     (apply update-routing-table (cons store node)))))
     results)
    results))

(define (without item lst)
  (filter (lambda (x) (not (equal? x item))) lst))

(define (server-find-node server target-id)
  (let* ((channel (gochan 0))
         (k-closest (with-store
                     (server-env server)
                     (lambda (store)
                       (n-closest-nodes store
                                        target-id
                                        (max-bucket-size))))))
    (let loop ((k-closest k-closest)
               (concurrency (concurrency))
               (running '())
               (ignored '())
               (remaining k-closest))
      (cond
       ;; spawn thread up to the concurrency limit
       ((and (not (null? remaining))
             (< (length running) concurrency))
        (let* ((node (car remaining))
               (thread (go-monitored
                        channel
                        (gochan-send channel
                                     (list 'thread-result
                                           (send-find-node server
                                                           (node-ip node)
                                                           (node-port node)
                                                           target-id))))))
          (loop k-closest
                concurrency
                (cons thread running)
                (cons (node-id node) ignored)
                (cdr remaining))))
       (else
        ;; wait for results
        (match (gochan-recv channel)
          (('thread-exit thread exn)
           (when exn
             ;; TODO: handle timeouts etc
             (abort exn))
           (let ((running (without thread running)))
             (cond
              ;; finished processing?
              ((and (null? remaining)
                    (null? running))
               ;; TODO: call rpc-cancel for remaining requests
               (for-each thread-terminate! running)
               k-closest)
              (else
               (loop k-closest
                     concurrency
                     running
                     ignored
                     remaining)))))
          (('thread-result nodes)
           (let* ((k-closest2 (take-closest-unique
                               (append (map (lambda (x)
                                              (make-node
                                               id: (first x)
                                               ip: (second x)
                                               port: (third x)))
                                            nodes)
                                       k-closest)
                               (max-bucket-size)
                               target-id))
                  (concurrency 
                   ;; check if distance to target stopped reducing
                   (if (u8vector<?
                        (distance (node-id (car k-closest2)) target-id)
                        (distance (node-id (car k-closest)) target-id))
                       concurrency
                       ;; if the distance didn't reduce, increase concurrency
                       ;; to max-bucket-size (k)
                       (max-bucket-size)))
                  (remaining (filter (lambda (node)
                                       (not (member (node-id node) ignored)))
                                     k-closest2)))
             (loop k-closest2
                   concurrency
                   running
                   ignored
                   remaining)))
          (msg
           (printf "unmatched message: ~S~n" msg)
           (abort 'fail))))))))

(define (server-join-network server)
  (assert (server-has-node? server))
  (server-find-node server (server-id server)))

)
