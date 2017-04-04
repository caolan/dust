(module dust.kademlia

;;;;; Exports ;;;;;
(make-node
 node-ip
 node-port
 node-id
 node->blob
 blob->node
 kademlia-env-open
 kademlia-store-open
 routing-table-open
 metadata-open
 local-id
 local-id-set!
 with-kademlia-store
 prefix->blob
 blob->prefix
 prefix-blob-compare
 node-blob-compare-first-seen
 k-bucket-nodes
 k-bucket-insert
 k-bucket-remove
 k-bucket-destroy
 k-bucket-split
 k-bucket-join
 k-bucket-size)

(import chicken scheme foreign)

(use lmdb-lolevel
     lazy-seq
     srfi-4
     data-structures
     sodium
     bitstring
     posix
     dust.bitstring-utils
     dust.lmdb-utils)

(foreign-declare "#include <lmdb.h>")
(foreign-declare "#include <string.h>")
(foreign-declare "#include <stdint.h>")

(define-record node
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
    ((node-first-seen node) 64 host signed)
    ((node-last-seen node) 64 host signed)
    ((node-failed-requests node) 8 host unsigned)
    ((node-id node) 160 bitstring)
    ((node-port node) 16 host unsigned)
    ((string->bitstring (node-ip node)) bitstring))))

(define (blob->node blob)
  (bitmatch blob
    (((first-seen-secs 64 host signed)
      (last-seen-secs 64 host signed)
      (failed-requests 8 host unsigned)
      (id 160 bitstring)
      (port 16 host unsigned)
      (ip bitstring))
     (make-node (bitstring->blob id)
                (bitstring->string ip)
                port
                first-seen-secs
                last-seen-secs
                failed-requests))))

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
    int64_t* a_first = a->mv_data;
    int64_t* b_first = b->mv_data;
    return (*a_first) - (*b_first);
  }")

(define node-blob-compare-first-seen
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

(define (k-bucket-nodes store prefix)
  ;; (printf "k-bucket-nodes: ~S ~S ~S~n" txn dbi prefix)
  (lazy-map blob->node (dup-values (kademlia-store-txn store)
                                   (kademlia-store-routing-table store)
                                   (prefix->blob prefix))))

(define (k-bucket-insert store prefix node)
  ;; (printf "k-bucket-insert: ~S ~S ~S ~S~n" txn dbi prefix node)
  (mdb-put (kademlia-store-txn store)
           (kademlia-store-routing-table store)
           (prefix->blob prefix)
           (node->blob node)
           0))

(define (k-bucket-remove store prefix node)
  ;; (printf "k-bucket-remove: ~S ~S ~S ~S~n" txn dbi prefix node)
  (mdb-del (kademlia-store-txn store)
           (kademlia-store-routing-table store)
           (prefix->blob prefix)
           (node->blob node)))

(define (k-bucket-destroy store prefix)
  ;; (printf "k-bucket-destroy: ~S ~S ~S~n" txn dbi prefix)
  (mdb-del (kademlia-store-txn store)
           (kademlia-store-routing-table store)
           (prefix->blob prefix)))

(define (k-bucket-split store prefix)
  ;; (printf "k-bucket-split: ~S ~S ~S~n" txn dbi prefix)
  (let ((upper (bitstring-append prefix (list->bitstring '(1))))
        (lower (bitstring-append prefix (list->bitstring '(0)))))
    (lazy-each (lambda (node)
                 (if (bitstring-bit-set?
                      (blob->bitstring (node-id node))
                      (bitstring-length prefix))
                     (k-bucket-insert store upper node)
                     (k-bucket-insert store lower node)))
               (k-bucket-nodes store prefix))
    (k-bucket-destroy store prefix)))

(define (k-bucket-join store parent)
  ;; (printf "k-bucket-join: ~S ~S ~S~n" txn dbi parent)
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

(define (k-bucket-size store prefix)
  (dup-count (kademlia-store-txn store)
             (kademlia-store-routing-table store)
             (prefix->blob prefix)))

)
