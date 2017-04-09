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
 kademlia-store-open
 kademlia-store-txn
 kademlia-store-routing-table
 kademlia-store-metadata
 routing-table-open
 metadata-open
 local-id
 local-id-set!
 with-kademlia-store
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
 update-routing-table)

(import chicken scheme foreign)

(use lmdb-lolevel
     lazy-seq
     srfi-4
     data-structures
     sodium
     posix
     bitstring
     defstruct
     dust.bitstring-utils
     dust.lazy-seq-utils
     dust.lmdb-utils)

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

(define-record kademlia-store
  txn
  routing-table
  metadata)

(define-record-printer (kademlia-store x out)
  (fprintf out "#<kademlia-store ~S ~S ~S>"
           (kademlia-store-txn x)
           (kademlia-store-routing-table x)
           (kademlia-store-metadata x)))

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
  (let ((env (mdb-env-create)))
    (mdb-env-set-mapsize env 10000000)
    (mdb-env-set-maxdbs env 2)
    (mdb-env-open env path MDB_NOSYNC
                  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    env))

(define (kademlia-store-open txn)
  (make-kademlia-store txn
                       (routing-table-open txn)
                       (metadata-open txn)))

(define (with-kademlia-store env thunk)
  (with-transaction env (compose thunk kademlia-store-open)))

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

(define (local-id-set! store id)
  (assert (= 20 (blob-size id)))
  (mdb-put (kademlia-store-txn store)
           (kademlia-store-metadata store)
           (string->blob "local-id")
           id
           0))

(define (local-id store)
  (condition-case
      (mdb-get (kademlia-store-txn store)
               (kademlia-store-metadata store)
               (string->blob "local-id"))
    ((exn lmdb MDB_NOTFOUND)
     (let ((new-id  (random-blob 20)))
       (local-id-set! store new-id)
       new-id))))

(define (bucket-nodes store prefix)
  ;; (printf "bucket-nodes: ~S ~S ~S~n" txn dbi prefix)
  (lazy-map blob->node (dup-values (kademlia-store-txn store)
                                   (kademlia-store-routing-table store)
                                   (prefix->blob prefix))))

(define (bucket-insert store prefix node)
  ;; (printf "bucket-insert: ~S ~S ~S ~S~n" txn dbi prefix node)
  (mdb-put (kademlia-store-txn store)
           (kademlia-store-routing-table store)
           (prefix->blob prefix)
           (node->blob node)
           0))

(define (bucket-remove store prefix node)
  ;; (printf "bucket-remove: ~S ~S ~S ~S~n" txn dbi prefix node)
  (mdb-del (kademlia-store-txn store)
           (kademlia-store-routing-table store)
           (prefix->blob prefix)
           (node->blob node)))

(define (bucket-destroy store prefix)
  ;; (printf "bucket-destroy: ~S ~S ~S~n" txn dbi prefix)
  (mdb-del (kademlia-store-txn store)
           (kademlia-store-routing-table store)
           (prefix->blob prefix)))

(define (bucket-split store prefix)
  ;; (printf "bucket-split: ~S ~S ~S~n" txn dbi prefix)
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
         (txn (kademlia-store-txn store))
         (dbi (kademlia-store-routing-table store)))
    (lazy-each (cut mdb-put txn dbi parent-key <> 0)
               (dup-values txn dbi child-0-key))
    (lazy-each (cut mdb-put txn dbi parent-key <> 0)
               (dup-values txn dbi child-1-key))
    (mdb-del txn dbi child-0-key)
    (mdb-del txn dbi child-1-key)))

(define (bucket-size store prefix)
  (dup-count (kademlia-store-txn store)
             (kademlia-store-routing-table store)
             (prefix->blob prefix)))

(define (bucket-find store prefix id)
  (lazy-find
   (lambda (node)
     (blob=? (node-id node) id))
   (bucket-nodes store prefix)))

(define (find-bucket-for-id store id)
  (let ((id (blob->bitstring id)))
    (with-cursor
     (kademlia-store-txn store)
     (kademlia-store-routing-table store)
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
  ;; (printf "update-routing-table: ~S ~S ~S ~S~n" store id ip port)
  (let ((prefix (find-bucket-for-id store id)))
    ;; (printf "prefix: ~S~n" prefix)
    (if prefix
        (let ((existing (bucket-find store prefix id)))
          ;; (printf "existing: ~S~n" existing)
          (bucket-insert store
                         prefix
                         (if existing
                             (update-node existing
                                          last-seen: (current-seconds))
                             (make-node id: id
                                        ip: ip
                                        port: port
                                        first-seen: (current-seconds)
                                        last-seen: (current-seconds)
                                        failed-requests: 0))))
        (bucket-insert store
                       (bitstring-take id 1)
                       (make-node id: id
                                  ip: ip
                                  port: port
                                  first-seen: (current-seconds)
                                  last-seen: (current-seconds)
                                  failed-requests: 0)))))

)
