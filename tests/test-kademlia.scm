(use test
     test-generative
     bitstring
     dust.kademlia
     lmdb-lolevel
     srfi-4
     lazy-seq
     sodium
     dust.lmdb-utils
     dust.bitstring-utils)

(define (clear-testdb #!optional (path "tests/testdb"))
  (when (file-exists? path)
    (delete-directory path #t))
  (create-directory path))

(define (with-test-store thunk)
  (clear-testdb)
  (let ((env (kademlia-env-open "tests/testdb")))
    (with-kademlia-store env thunk)
    (mdb-env-close env)))

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

(define ((random-bitstring max-len))
  (let ((blob (random-blob (+ 1 (random (fx/ max-len 8))))))
    (bitstring-take (blob->bitstring blob)
                    (+ 1 (random (* 8 (blob-size blob)))))))

(test-group "prefix-blob-compare"
  (test-assert
      (zero? (prefix-blob-compare
              (prefix->blob (list->bitstring '(1 0 1)))
              (prefix->blob (list->bitstring '(1 0 1))))))
  (test-assert
      (zero? (prefix-blob-compare
              (prefix->blob (list->bitstring '()))
              (prefix->blob (list->bitstring '())))))
  (test-assert
      (zero? (prefix-blob-compare
              (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0)))
              (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0))))))
  (test-assert
      (positive? (prefix-blob-compare
                  (prefix->blob (list->bitstring '(1 1 0)))
                  (prefix->blob (list->bitstring '())))))
  (test-assert
      (positive? (prefix-blob-compare
                  (prefix->blob (list->bitstring '(1 1 0)))
                  (prefix->blob (list->bitstring '(1 1))))))
  (test-assert
      (positive? (prefix-blob-compare
                  (prefix->blob (list->bitstring '(1 1 0)))
                  (prefix->blob (list->bitstring '(1 0 1))))))
  (test-assert
      (positive? (prefix-blob-compare
                  (prefix->blob (list->bitstring '(1 1 0)))
                  (prefix->blob (list->bitstring '(0 1 0 1))))))
  (test-assert
      (positive? (prefix-blob-compare
                  (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0 1)))
                  (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0))))))
  (test-assert
      (negative? (prefix-blob-compare
                  (prefix->blob (list->bitstring '()))
                  (prefix->blob (list->bitstring '(1 1 0))))))
  (test-assert
      (negative? (prefix-blob-compare
                  (prefix->blob (list->bitstring '(1 1)))
                  (prefix->blob (list->bitstring '(1 1 0))))))
  (test-assert
      (negative? (prefix-blob-compare
                  (prefix->blob (list->bitstring '(1 0 1)))
                  (prefix->blob (list->bitstring '(1 1 0))))))
  (test-assert
      (negative? (prefix-blob-compare
                  (prefix->blob (list->bitstring '(0 1 0 1)))
                  (prefix->blob (list->bitstring '(1 1 0))))))
  (test-assert
      (negative? (prefix-blob-compare
                  (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0)))
                  (prefix->blob (list->bitstring '(1 1 0 1 0 1 0 1 1 1 0 1))))))
  (parameterize
      ((current-test-generative-iterations 1000))
    (test-generative ((a (random-bitstring 160))
                      (b (random-bitstring 160)))
      (let ((x (bitstring-compare a b))
            (y (prefix-blob-compare (prefix->blob a) (prefix->blob b))))
        (test "test against bitstring-compare (using random bitstrings)"
              (cond ((zero? x) 'zero)
                    ((positive? x) 'positive)
                    ((negative? x) 'negative))
              (cond ((zero? y) 'zero)
                    ((positive? y) 'positive)
                    ((negative? y) 'negative)))))))

(test-group "node->blob and blob->node"
  (let ((node (make-node id: (u8vector->blob/shared (make-u8vector 20 0))
                         ip: "192.168.0.101"
                         port: 4219
                         first-seen: 100
                         last-seen: 456
                         failed-requests: 0)))
    (test-assert (blob? (node->blob node)))
    (test node (blob->node (node->blob node)))))

(test-group "bucket-insert"
  (let ((node1 (make-node id: #${0000000000000000000000000000000000000000}
                          ip: "192.168.0.101"
                          port: 4219
                          first-seen: 101
                          last-seen: 456
                          failed-requests: 0))
        (node2 (make-node id: #${2000000000000000000000000000000000000000}
                          ip: "192.168.0.102"
                          port: 14220
                          first-seen: 102
                          last-seen: 456
                          failed-requests: 0))
        (node3 (make-node id: #${8000000000000000000000000000000000000000}
                          ip: "192.168.0.103"
                          port: 8000
                          first-seen: 103
                          last-seen: 456
                          failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0 0)) node1)
       (bucket-insert store (list->bitstring '(0 0)) node2)
       (bucket-insert store (list->bitstring '(1)) node3)
       (test (list node1 node2)
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0 0)))))
       (test (list node3)
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(1)))))))))

(test-group "bucket-remove"
  (let ((node1 (make-node id: #${0000000000000000000000000000000000000000}
                          ip: "192.168.0.101"
                          port: 4219
                          first-seen: 101
                          last-seen: 456
                          failed-requests: 0))
        (node2 (make-node id: #${2000000000000000000000000000000000000000}
                          ip: "192.168.0.102"
                          port: 14220
                          first-seen: 102
                          last-seen: 456
                          failed-requests: 0))
        (node3 (make-node id: #${4000000000000000000000000000000000000000}
                          ip: "192.168.0.103"
                          port: 8000
                          first-seen: 103
                          last-seen: 456
                          failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0)) node1)
       (bucket-insert store (list->bitstring '(0)) node2)
       (bucket-insert store (list->bitstring '(0)) node3)
       (bucket-remove store (list->bitstring '(0)) node2)
       (test (list node1 node3)
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0)))))))))

(test-group "bucket-destroy"
  (let ((node1 (make-node id: #${0000000000000000000000000000000000000000}
                          ip: "192.168.0.101"
                          port: 4219
                          first-seen: 101
                          last-seen: 456
                          failed-requests: 0))
        (node2 (make-node id: #${2000000000000000000000000000000000000000}
                          ip: "192.168.0.102"
                          port: 14220
                          first-seen: 102
                          last-seen: 456
                          failed-requests: 0))
        (node3 (make-node id: #${8000000000000000000000000000000000000000}
                          ip: "192.168.0.103"
                          port: 8000
                          first-seen: 103
                          last-seen: 456
                          failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0 0)) node1)
       (bucket-insert store (list->bitstring '(0 0)) node2)
       (bucket-insert store (list->bitstring '(1)) node3)
       (bucket-destroy store (list->bitstring '(0 0)))
       (test '()
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0 0)))))
       (test (list node3)
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(1)))))))))

(test-group "bucket-split"
  (let ((node1 (make-node id: #${0000000000000000000000000000000000000000}
                          ip: "192.168.0.101"
                          port: 4219
                          first-seen: 101
                          last-seen: 456
                          failed-requests: 0))
        (node2 (make-node id: #${2000000000000000000000000000000000000000}
                          ip: "192.168.0.102"
                          port: 14220
                          first-seen: 102
                          last-seen: 456
                          failed-requests: 0))
        (node3 (make-node id: #${4000000000000000000000000000000000000000}
                          ip: "192.168.0.103"
                          port: 8000
                          first-seen: 103
                          last-seen: 456
                          failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0)) node1)
       (bucket-insert store (list->bitstring '(0)) node2)
       (bucket-insert store (list->bitstring '(0)) node3)
       (bucket-split store (list->bitstring '(0)))
       (test (list node1 node2)
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0 0)))))
       (test (list node3)
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0 1)))))
       (test '()
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0)))))))))

(test-group "bucket-join"
  (let ((node1 (make-node id: #${0000000000000000000000000000000000000000}
                          ip: "192.168.0.101"
                          port: 4219
                          first-seen: 101
                          last-seen: 456
                          failed-requests: 0))
        (node2 (make-node id: #${2000000000000000000000000000000000000000}
                          ip: "192.168.0.102"
                          port: 14220
                          first-seen: 102
                          last-seen: 456
                          failed-requests: 0))
        (node3 (make-node id: #${4000000000000000000000000000000000000000}
                          ip: "192.168.0.103"
                          port: 8000
                          first-seen: 103
                          last-seen: 456
                          failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0 0)) node1)
       (bucket-insert store (list->bitstring '(0 0)) node2)
       (bucket-insert store (list->bitstring '(0 1)) node3)
       (bucket-join store (list->bitstring '(0)))
       (test '()
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0 0)))))
       (test '()
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0 1)))))
       (test (list node1 node2 node3)
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0)))))))))

(test-group "bucket-size"
  (let ((node1 (make-node id: #${0000000000000000000000000000000000000000}
                          ip: "192.168.0.101"
                          port: 4219
                          first-seen: 101
                          last-seen: 456
                          failed-requests: 0))
        (node2 (make-node id: #${2000000000000000000000000000000000000000}
                          ip: "192.168.0.102"
                          port: 14220
                          first-seen: 102
                          last-seen: 456
                          failed-requests: 0))
        (node3 (make-node id: #${4000000000000000000000000000000000000000}
                          ip: "192.168.0.103"
                          port: 8000
                          first-seen: 103
                          last-seen: 456
                          failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0 0)) node1)
       (bucket-insert store (list->bitstring '(0 0)) node2)
       (bucket-insert store (list->bitstring '(0 1)) node3)
       (test 2 (bucket-size store (list->bitstring '(0 0))))
       (test 1 (bucket-size store (list->bitstring '(0 1))))))))

;; (test-group "bucket are sorted by first-seen"
;;   (let ((node1 (make-node id: #${0000000000000000000000000000000000000000}
;;                           ip: "192.168.0.101"
;;                           port: 4219
;;                           first-seen: 103
;;                           last-seen: 456
;;                           failed-requests: 1))
;;         (node2 (make-node id: #${2000000000000000000000000000000000000000}
;;                           ip: "192.168.0.102"
;;                           port: 14220
;;                           first-seen: 101
;;                           last-seen: 456
;;                           failed-requests: 2))
;;         (node3 (make-node id: #${4000000000000000000000000000000000000000}
;;                           ip: "192.168.0.103"
;;                           port: 8000
;;                           first-seen: 102
;;                           last-seen: 456
;;                           failed-requests: 3)))
;;     (with-test-store
;;      (lambda (store)
;;        (bucket-insert store (list->bitstring '(0)) node1)
;;        (bucket-insert store (list->bitstring '(0)) node2)
;;        (bucket-insert store (list->bitstring '(0)) node3)
;;        (test (list node2 node3 node1)
;;              (lazy-seq->list
;;               (bucket-nodes store (list->bitstring '(0)))))))))

(test-group "inserting same id into bucket replaces original"
  (let ((node1 (make-node id: #${0000000000000000000000000000000000000000}
                          ip: "192.168.0.101"
                          port: 4219
                          first-seen: 103
                          last-seen: 456
                          failed-requests: 1))
        (node2 (make-node id: #${0000000000000000000000000000000000000000}
                          ip: "192.168.0.102"
                          port: 14220
                          first-seen: 101
                          last-seen: 456
                          failed-requests: 2)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0)) node1)
       (bucket-insert store (list->bitstring '(0)) node2)
       (test (list node2)
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0)))))))))

(test-group "local-id / local-id-set!"
  (with-test-store
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
             (local-id store))))))

(test-group "find-bucket-for-id"
  (with-test-store
   (lambda (store)
     (test #f
           (find-bucket-for-id
            store #${0000000000000000000000000000000000000000}))
     (bucket-insert store
                      (list->bitstring '(0))
                      (make-node id: #${0000000000000000000000000000000000000000}
                                 ip: "192.168.0.101"
                                 port: 4219
                                 first-seen: 100
                                 last-seen: 200
                                 failed-requests: 0))
     (test-assert
         (bitstring=?
          (list->bitstring '(0))
          (find-bucket-for-id
           store #${0000000000000000000000000000000000000000})))
     (bucket-insert store
                      (list->bitstring '(1))
                      (make-node id: #${ffffffffffffffffffffffffffffffffffffffff}
                                 ip: "192.168.0.102"
                                 port: 4219
                                 first-seen: 100
                                 last-seen: 200
                                 failed-requests: 0))
     (test-assert
         (bitstring=?
          (list->bitstring '(1))
          (find-bucket-for-id
           store #${ffffffffffffffffffffffffffffffffffffffff})))
     (bucket-insert store
                      (list->bitstring '(0 1 0 1))
                      (make-node id: #${5555555555555555555555555555555555555555}
                                 ip: "192.168.0.103"
                                 port: 4219
                                 first-seen: 100
                                 last-seen: 200
                                 failed-requests: 0))
     (bucket-insert store
                      (list->bitstring '(0 1 1 1))
                      (make-node id: #${7fffffffffffffffffffffffffffffffffffffff}
                                 ip: "192.168.0.103"
                                 port: 4219
                                 first-seen: 100
                                 last-seen: 200
                                 failed-requests: 0))
     (test-assert
         (bitstring=?
          (list->bitstring '(0 1 0 1))
          (find-bucket-for-id
           store #${5555555555555555555555555555555555555555})))
     (test "sanity check: routing-table keys are in expected order"
           '((0)
             (0 1 0 1)
             (0 1 1 1)
             (1))
           (lazy-seq->list
            (lazy-map (lambda (k)
                        (bitstring->list (blob->prefix k)))
                      (keys (kademlia-store-txn store)
                            (kademlia-store-routing-table store)))))
     (test-assert
         (bitstring=?
          (list->bitstring '(0 1 1 1))
          (find-bucket-for-id
           store #${7fffffffffffffffffffffffffffffffffffffff})))
     )))
     
(test-group "update-routing-table: insert into empty table"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          #${0000000000000000000000000000000000000000}
          "192.168.0.101"
          4219))
       (test (list (make-node id: #${0000000000000000000000000000000000000000}
                              ip: "192.168.0.101"
                              port: 4219
                              first-seen: 300
                              last-seen: 300
                              failed-requests: 0))
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0)))))
       ))))

;; TODO: make sure failed-requests isn't reset too?
(test-group "update-routing-table: insert existing node"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          #${0000000000000000000000000000000000000000}
          "192.168.0.101"
          4219))
       (fluid-let ((current-seconds (cut identity 400)))
         (update-routing-table
          store
          #${0000000000000000000000000000000000000000}
          "192.168.0.101"
          4219))
       (test (list (make-node id: #${0000000000000000000000000000000000000000}
                              ip: "192.168.0.101"
                              port: 4219
                              first-seen: 300
                              last-seen: 400
                              failed-requests: 0))
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0)))))
       ))))

(test-group "update-routing-table: insert new node into existing bucket"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          #${0000000000000000000000000000000000000000}
          "192.168.0.101"
          4219))
       (fluid-let ((current-seconds (cut identity 400)))
         (update-routing-table
          store
          #${5555555555555555555555555555555555555555}
          "192.168.0.102"
          4219))
       (test (list (make-node id: #${0000000000000000000000000000000000000000}
                              ip: "192.168.0.101"
                              port: 4219
                              first-seen: 300
                              last-seen: 300
                              failed-requests: 0)
                   (make-node id: #${5555555555555555555555555555555555555555}
                              ip: "192.168.0.102"
                              port: 4219
                              first-seen: 400
                              last-seen: 400
                              failed-requests: 0))
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0)))))
       ))))

(test-group "update-routing-table: insert into existing bucket - same first-seen"
  ;; nodes with same first-seen values should then sort on node-id
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          #${0000000000000000000000000000000000000000}
          "192.168.0.101"
          4219)
         (update-routing-table
          store
          #${5555555555555555555555555555555555555555}
          "192.168.0.102"
          4219))
       (test (list (make-node id: #${0000000000000000000000000000000000000000}
                              ip: "192.168.0.101"
                              port: 4219
                              first-seen: 300
                              last-seen: 300
                              failed-requests: 0)
                   (make-node id: #${5555555555555555555555555555555555555555}
                              ip: "192.168.0.102"
                              port: 4219
                              first-seen: 300
                              last-seen: 300
                              failed-requests: 0))
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0)))))
       ))))

(test-group "update-routing-table: insert new node into new bucket"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          #${0000000000000000000000000000000000000000}
          "192.168.0.101"
          4219))
       (fluid-let ((current-seconds (cut identity 400)))
         (update-routing-table
          store
          #${ffffffffffffffffffffffffffffffffffffffff}
          "192.168.0.102"
          4219))
       (test (list (make-node id: #${0000000000000000000000000000000000000000}
                              ip: "192.168.0.101"
                              port: 4219
                              first-seen: 300
                              last-seen: 300
                              failed-requests: 0))
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(0)))))
       (test (list (make-node id: #${ffffffffffffffffffffffffffffffffffffffff}
                              ip: "192.168.0.102"
                              port: 4219
                              first-seen: 400
                              last-seen: 400
                              failed-requests: 0))
             (lazy-seq->list
              (bucket-nodes store (list->bitstring '(1)))))
       ))))

(test-group "update-routing-table: split bucket"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       (parameterize
           ;; use a small 'k' parameter (bucket size)
           ((max-bucket-size 3))
         ;;  use a fixed current time for comparisons
         (fluid-let ((current-seconds (cut identity 300)))
           (update-routing-table
            store
            #${ffffffffffffffffffffffffffffffffffffffff} ; 11111111
            "192.168.0.101"
            4219)
           (update-routing-table
            store
            #${cccccccccccccccccccccccccccccccccccccccc} ; 11001100
            "192.168.0.102"
            4219)
           (update-routing-table
            store
            #${bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb} ; 10111011
            "192.168.0.103"
            4219)
           (update-routing-table
            store
            #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa} ; 10101010
            "192.168.0.104"
            4219))
         (test '()
               (lazy-seq->list
                (bucket-nodes store (list->bitstring '(0)))))
         (test (list (make-node id: #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}
                                ip: "192.168.0.104"
                                port: 4219
                                first-seen: 300
                                last-seen: 300
                                failed-requests: 0)
                     (make-node id: #${bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}
                                ip: "192.168.0.103"
                                port: 4219
                                first-seen: 300
                                last-seen: 300
                                failed-requests: 0))
               (lazy-seq->list
                (bucket-nodes store (list->bitstring '(1 0)))))
         (test (list (make-node id: #${cccccccccccccccccccccccccccccccccccccccc}
                                ip: "192.168.0.102"
                                port: 4219
                                first-seen: 300
                                last-seen: 300
                                failed-requests: 0)
                     (make-node id: #${ffffffffffffffffffffffffffffffffffffffff}
                                ip: "192.168.0.101"
                                port: 4219
                                first-seen: 300
                                last-seen: 300
                                failed-requests: 0))
               (lazy-seq->list
                (bucket-nodes store (list->bitstring '(1 1)))))
         (test '()
               (lazy-seq->list
                (bucket-nodes store (list->bitstring '(1)))))
         )))))

(test-group "update-routing-table: don't split bucket"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       (parameterize
           ;; use a small 'k' parameter (bucket size)
           ((max-bucket-size 3))
         ;;  use a fixed current time for comparisons
         (fluid-let ((current-seconds (cut identity 300)))
           (update-routing-table
            store
            #${0000000000000000000000000000000000000000} ; 00000000
            "192.168.0.101"
            4219))
         (fluid-let ((current-seconds (cut identity 400)))
           (update-routing-table
            store
            #${3333333333333333333333333333333333333333} ; 00110011
            "192.168.0.102"
            4219))
         (fluid-let ((current-seconds (cut identity 500)))
           (update-routing-table
            store
            #${4444444444444444444444444444444444444444} ; 01000100
            "192.168.0.103"
            4219))
         (fluid-let ((current-seconds (cut identity 600)))
           (update-routing-table
            store
            #${5555555555555555555555555555555555555555} ; 01010101
            "192.168.0.104"
            4219))
         (test (list (make-node id: #${0000000000000000000000000000000000000000}
                                ip: "192.168.0.101"
                                port: 4219
                                first-seen: 300
                                last-seen: 300
                                failed-requests: 0)
                     (make-node id: #${3333333333333333333333333333333333333333}
                                ip: "192.168.0.102"
                                port: 4219
                                first-seen: 400
                                last-seen: 400
                                failed-requests: 0)
                     (make-node id: #${4444444444444444444444444444444444444444}
                                ip: "192.168.0.103"
                                port: 4219
                                first-seen: 500
                                last-seen: 500
                                failed-requests: 0))
               (lazy-seq->list
                (bucket-nodes store (list->bitstring '(0)))))
         (test '()
               (lazy-seq->list
                (bucket-nodes store (list->bitstring '(1)))))
         )))))

(test-exit)
