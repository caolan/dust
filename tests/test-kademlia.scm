(use test bitstring dust.kademlia lmdb-lolevel srfi-4 lazy-seq)

(define (clear-testdb #!optional (path "tests/testdb"))
  (when (file-exists? path)
    (delete-directory path #t))
  (create-directory path))

(test-group "prefix->blob and blob->prefix"
  (test-assert (blob? (prefix->blob (list->bitstring '(1 0 0 1 1)))))
  (test-assert
      (bitstring=?
       (list->bitstring '(1 0 0 1 1))
       (blob->prefix (prefix->blob (list->bitstring '(1 0 0 1 1))))))
  (let* ((blob (make-blob 20))
         (bstr (blob->bitstring blob)))
    (test-assert
        (bitstring=? bstr (blob->prefix (prefix->blob bstr))))))

(test-group "prefix-blob-compare"
  (test 0 (prefix-blob-compare
           (prefix->blob (list->bitstring '(1 0 1)))
           (prefix->blob (list->bitstring '(1 0 1)))))
  (test 0 (prefix-blob-compare
           (prefix->blob (list->bitstring '()))
           (prefix->blob (list->bitstring '()))))
  (test 0 (prefix-blob-compare
           (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0)))
           (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0)))))
  (test 1 (prefix-blob-compare
           (prefix->blob (list->bitstring '(1 1 0)))
           (prefix->blob (list->bitstring '()))))
  (test 1 (prefix-blob-compare
           (prefix->blob (list->bitstring '(1 1 0)))
           (prefix->blob (list->bitstring '(1 1)))))
  (test 1 (prefix-blob-compare
           (prefix->blob (list->bitstring '(1 1 0)))
           (prefix->blob (list->bitstring '(1 0 1)))))
  (test 1 (prefix-blob-compare
           (prefix->blob (list->bitstring '(1 1 0)))
           (prefix->blob (list->bitstring '(0 1 0 1)))))
  (test 1 (prefix-blob-compare
           (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0 1)))
           (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0)))))
  (test -1 (prefix-blob-compare
            (prefix->blob (list->bitstring '()))
            (prefix->blob (list->bitstring '(1 1 0)))))
  (test -1 (prefix-blob-compare
            (prefix->blob (list->bitstring '(1 1)))
            (prefix->blob (list->bitstring '(1 1 0)))))
  (test -1 (prefix-blob-compare
            (prefix->blob (list->bitstring '(1 0 1)))
            (prefix->blob (list->bitstring '(1 1 0)))))
  (test -1 (prefix-blob-compare
            (prefix->blob (list->bitstring '(0 1 0 1)))
            (prefix->blob (list->bitstring '(1 1 0)))))
  (test -1 (prefix-blob-compare
            (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0)))
            (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0 1))))))

(test-group "node->blob and blob->node"
  (let ((node (make-node (u8vector->blob/shared (make-u8vector 20 0))
                         "192.168.0.101"
                         4219
                         100
                         456
                         0)))
    (test-assert (blob? (node->blob node)))
    (test node (blob->node (node->blob node)))))

(test-group "k-bucket-insert"
  (clear-testdb)
  (let ((node1 (make-node #${0000000000000000000000000000000000000000}
                          "192.168.0.101"
                          4219
                          101
                          456
                          0))
        (node2 (make-node #${2000000000000000000000000000000000000000}
                          "192.168.0.102"
                          14220
                          102
                          456
                          0))
        (node3 (make-node #${8000000000000000000000000000000000000000}
                          "192.168.0.103"
                          8000
                          103
                          456
                          0)))
    (let* ((env (kademlia-env-open "tests/testdb")))
      (with-kademlia-store
       env
       (lambda (store)
         (k-bucket-insert store (list->bitstring '(0 0)) node1)
         (k-bucket-insert store (list->bitstring '(0 0)) node2)
         (k-bucket-insert store (list->bitstring '(1)) node3)
         (test (list node1 node2)
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0 0)))))
         (test (list node3)
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(1)))))))
      (mdb-env-close env))))

(test-group "k-bucket-remove"
  (clear-testdb)
  (let ((node1 (make-node #${0000000000000000000000000000000000000000}
                          "192.168.0.101"
                          4219
                          101
                          456
                          0))
        (node2 (make-node #${2000000000000000000000000000000000000000}
                          "192.168.0.102"
                          14220
                          102
                          456
                          0))
        (node3 (make-node #${4000000000000000000000000000000000000000}
                          "192.168.0.103"
                          8000
                          103
                          456
                          0)))
    (let* ((env (kademlia-env-open "tests/testdb")))
      (with-kademlia-store
       env
       (lambda (store)
         (k-bucket-insert store (list->bitstring '(0)) node1)
         (k-bucket-insert store (list->bitstring '(0)) node2)
         (k-bucket-insert store (list->bitstring '(0)) node3)
         (k-bucket-remove store (list->bitstring '(0)) node2)
         (test (list node1 node3)
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0)))))))
      (mdb-env-close env))))

(test-group "k-bucket-destroy"
  (clear-testdb)
  (let ((node1 (make-node #${0000000000000000000000000000000000000000}
                          "192.168.0.101"
                          4219
                          101
                          456
                          0))
        (node2 (make-node #${2000000000000000000000000000000000000000}
                          "192.168.0.102"
                          14220
                          102
                          456
                          0))
        (node3 (make-node #${8000000000000000000000000000000000000000}
                          "192.168.0.103"
                          8000
                          103
                          456
                          0)))
    (let* ((env (kademlia-env-open "tests/testdb")))
      (with-kademlia-store
       env
       (lambda (store)
         (k-bucket-insert store (list->bitstring '(0 0)) node1)
         (k-bucket-insert store (list->bitstring '(0 0)) node2)
         (k-bucket-insert store (list->bitstring '(1)) node3)
         (k-bucket-destroy store (list->bitstring '(0 0)))
         (test '()
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0 0)))))
         (test (list node3)
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(1)))))))
      (mdb-env-close env))))

(test-group "k-bucket-split"
  (clear-testdb)
  (let ((node1 (make-node #${0000000000000000000000000000000000000000}
                          "192.168.0.101"
                          4219
                          101
                          456
                          0))
        (node2 (make-node #${2000000000000000000000000000000000000000}
                          "192.168.0.102"
                          14220
                          102
                          456
                          0))
        (node3 (make-node #${4000000000000000000000000000000000000000}
                          "192.168.0.103"
                          8000
                          103
                          456
                          0)))
    (let* ((env (kademlia-env-open "tests/testdb")))
      (with-kademlia-store
       env
       (lambda (store)
         (k-bucket-insert store (list->bitstring '(0)) node1)
         (k-bucket-insert store (list->bitstring '(0)) node2)
         (k-bucket-insert store (list->bitstring '(0)) node3)
         (k-bucket-split store (list->bitstring '(0)))
         (test (list node1 node2)
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0 0)))))
         (test (list node3)
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0 1)))))
         (test '()
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0)))))))
      (mdb-env-close env))))

(test-group "k-bucket-join"
  (clear-testdb)
  (let ((node1 (make-node #${0000000000000000000000000000000000000000}
                          "192.168.0.101"
                          4219
                          101
                          456
                          0))
        (node2 (make-node #${2000000000000000000000000000000000000000}
                          "192.168.0.102"
                          14220
                          102
                          456
                          0))
        (node3 (make-node #${4000000000000000000000000000000000000000}
                          "192.168.0.103"
                          8000
                          103
                          456
                          0)))
    (let* ((env (kademlia-env-open "tests/testdb")))
      (with-kademlia-store
       env
       (lambda (store)
         (k-bucket-insert store (list->bitstring '(0 0)) node1)
         (k-bucket-insert store (list->bitstring '(0 0)) node2)
         (k-bucket-insert store (list->bitstring '(0 1)) node3)
         (k-bucket-join store (list->bitstring '(0)))
         (test '()
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0 0)))))
         (test '()
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0 1)))))
         (test (list node1 node2 node3)
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0)))))))
      (mdb-env-close env))))

(test-group "k-bucket-size"
  (clear-testdb)
  (let ((node1 (make-node #${0000000000000000000000000000000000000000}
                          "192.168.0.101"
                          4219
                          101
                          456
                          0))
        (node2 (make-node #${2000000000000000000000000000000000000000}
                          "192.168.0.102"
                          14220
                          102
                          456
                          0))
        (node3 (make-node #${4000000000000000000000000000000000000000}
                          "192.168.0.103"
                          8000
                          103
                          456
                          0)))
    (let* ((env (kademlia-env-open "tests/testdb")))
      (with-kademlia-store
       env
       (lambda (store)
         (k-bucket-insert store (list->bitstring '(0 0)) node1)
         (k-bucket-insert store (list->bitstring '(0 0)) node2)
         (k-bucket-insert store (list->bitstring '(0 1)) node3)
         (test 2 (k-bucket-size store (list->bitstring '(0 0))))
         (test 1 (k-bucket-size store (list->bitstring '(0 1))))))
      (mdb-env-close env))))

(test-group "k-bucket are sorted by first-seen"
  (clear-testdb)
  (let ((node1 (make-node #${0000000000000000000000000000000000000000}
                          "192.168.0.101"
                          4219
                          103
                          456
                          1))
        (node2 (make-node #${2000000000000000000000000000000000000000}
                          "192.168.0.102"
                          14220
                          101
                          456
                          2))
        (node3 (make-node #${4000000000000000000000000000000000000000}
                          "192.168.0.103"
                          8000
                          102
                          456
                          3)))
    (let* ((env (kademlia-env-open "tests/testdb")))
      (with-kademlia-store
       env
       (lambda (store)
         (k-bucket-insert store (list->bitstring '(0)) node1)
         (k-bucket-insert store (list->bitstring '(0)) node2)
         (k-bucket-insert store (list->bitstring '(0)) node3)
         (test (list node2 node3 node1)
               (lazy-seq->list
                (k-bucket-nodes store (list->bitstring '(0)))))))
      (mdb-env-close env))))

(test-group "local-id / local-id-set!"
  (clear-testdb)
  (let* ((env (kademlia-env-open "tests/testdb")))
    (with-kademlia-store
     env
     (lambda (store)
       (let ((test-id #${0102030405060708090001020304050607080900})
             (first-id (local-id store)))
         (test-assert "set random local-id on first call"
           (blob? first-id))
         (test 20 (blob-size first-id))
         (test "second call returns originally generated id"
               first-id
               (local-id store))
         (local-id-set! store test-id)
         (test "get after setting id manually"
               test-id
               (local-id store)))))
    (mdb-env-close env)))
