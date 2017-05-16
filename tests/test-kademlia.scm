(use test
     test-generative
     data-generators
     bitstring
     dust.kademlia
     lmdb-lolevel
     posix
     extras
     files
     srfi-4
     lazy-seq
     sodium
     gochan
     log5scm
     matchable
     miscmacros
     dust.lmdb-utils
     dust.u8vector-utils
     dust.bitstring-utils)


(define (clear-testdb #!optional (path "tests/testdb"))
  (when (file-exists? path)
    (delete-directory path #t))
  (create-directory path #t))

(define (with-test-store thunk)
  (clear-testdb)
  (let ((env (kademlia-env-open "tests/testdb")))
    (with-store env thunk)
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
                      (keys (store-txn store)
                            (store-routing-table store)))))
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
         (test "new entry is discarded"
               (list (make-node id: #${0000000000000000000000000000000000000000}
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

(test-group "update-routing-table: inserting existing node into full bucket updates last-seen"
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
            #${0000000000000000000000000000000000000000} ; 01010101
            "192.168.0.104"
            4219))
         (test "new entry is discarded"
               (list (make-node id: #${0000000000000000000000000000000000000000}
                                ip: "192.168.0.101"
                                port: 4219
                                first-seen: 300
                                last-seen: 600
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

(test-group "distance"
  (let ((id0 (bitstring->blob (list->bitstring (make-list 160 0))))
        (id1 (let ((v (make-vector 160 0)))
               (vector-set! v 159 1)
               (bitstring->blob (list->bitstring (vector->list v)))))
        (id2 (let ((v (make-vector 160 0)))
               (vector-set! v 158 1)
               (bitstring->blob (list->bitstring (vector->list v)))))
        (id4 (let ((v (make-vector 160 0)))
               (vector-set! v 157 1)
               (bitstring->blob (list->bitstring (vector->list v)))))
        (id5 (let ((v (make-vector 160 0)))
               (vector-set! v 159 1)
               (vector-set! v 157 1)
               (bitstring->blob
                (list->bitstring (vector->list v))))))
    (test "uses XOR"
          '(1 0 1 1 1 0 0 1)
          (bitstring->list
           (blob->bitstring
            (u8vector->blob
             (distance
              (bitstring->blob (list->bitstring '(1 0 0 1 0 0 1 1)))
              (bitstring->blob (list->bitstring '(0 0 1 0 1 0 1 0))))))))
    (test #u8(0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 3) (distance id1 id2))
    (test #u8(0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1) (distance id1 id0))
    (test #u8(0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 4) (distance id4 id0))
    (test #u8(0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0) (distance id5 id5))
    (test-assert
        (u8vector<?
         (distance id4 id1)
         (distance id4 #${ff00000000000000000000000000000000000000})))))

(define (with-test-servers configs thunk #!optional (servers '()))
  (if (null? configs)
   (apply thunk (reverse servers))
   (match (car configs)
     ((id host port)
      (let ((db-path (make-pathname "tests/server-dbs/" (bin->hex id))))
        (clear-testdb db-path)
        (with-server
         db-path
         host
         port
         (lambda (server)
           (server-id-set! server id)
           (with-test-servers
            (cdr configs) thunk (cons server servers)))))))))

(test-group "PING"
  (with-test-servers
   '((#${0000000000000000000000000000000000000000} "127.0.0.1" 4218)
     (#${ffffffffffffffffffffffffffffffffffffffff} "127.0.0.1" 4217))
   (lambda (server1 server2)
     (test #${ffffffffffffffffffffffffffffffffffffffff}
           (send-ping server1 "127.0.0.1" 4217))
     (test #${0000000000000000000000000000000000000000}
           (send-ping server2 "127.0.0.1" 4218)))))

(test-group "simple STORE and FIND_VALUE"
  (with-test-servers
   '((#${0000000000000000000000000000000000000000} "127.0.0.1" 4218)
     (#${ffffffffffffffffffffffffffffffffffffffff} "127.0.0.1" 4217))
   (lambda (server1 server2)
     ;; (store-rpc server2 "127.0.0.1" 4218 "key-one" "value-one")
     ;; (store-rpc server1 "127.0.0.1" 4217 "key-two" "value-two")
     (send-store server2 "127.0.0.1" 4217 "key-one" "value-one")
     (send-store server1 "127.0.0.1" 4218 "key-two" "value-two")
     ;; (test "value-one" (find-value-rpc server2 "127.0.0.1" 4218 "key-one"))
     ;; (test "value-two" (find-value-rpc server1 "127.0.0.1" 4217 "key-two")))))
     (test "value-one" (send-find-value server2 "127.0.0.1" 4217 "key-one"))
     (test "value-two" (send-find-value server1 "127.0.0.1" 4218 "key-two")))))

;; given the 8 bits of the first byte, make a full node id by
;; repeating them
(define (make-id . bits)
  (assert (= 8 (length bits)))
  (bitstring->blob (list->bitstring (join (make-list 20 bits)))))

(test-group "FIND_NODE returns all nodes if < k available"
  ;; (start-sender
  ;;  catchall-sender
  ;;  (port-sender (current-error-port))
  ;;  (category (debug kademlia)))
  (with-test-servers
   `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201)
     (,(make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202))
   (lambda (s1 s2)
     (server-add-node s1 (make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202)
     (server-add-node s1 (make-id 0 0 0 0 0 1 0 0) "127.0.0.1" 4203)
     (server-add-node s1 (make-id 0 0 0 0 1 0 0 0) "127.0.0.1" 4204)
     (server-add-node s1 (make-id 0 0 0 1 0 0 0 0) "127.0.0.1" 4205)
     (server-add-node s1 (make-id 0 0 1 0 0 0 0 0) "127.0.0.1" 4206)
     (server-add-node s1 (make-id 0 1 0 0 0 0 0 0) "127.0.0.1" 4207)
     (server-add-node s1 (make-id 1 0 0 0 0 0 0 0) "127.0.0.1" 4208)
     (test `((,(make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202)
             (,(make-id 0 0 0 0 0 1 0 0) "127.0.0.1" 4203)
             (,(make-id 0 0 0 0 1 0 0 0) "127.0.0.1" 4204)
             (,(make-id 0 0 0 1 0 0 0 0) "127.0.0.1" 4205)
             (,(make-id 0 0 1 0 0 0 0 0) "127.0.0.1" 4206)
             (,(make-id 0 1 0 0 0 0 0 0) "127.0.0.1" 4207)
             (,(make-id 1 0 0 0 0 0 0 0) "127.0.0.1" 4208))
           (send-find-node s2 "127.0.0.1" 4201 (server-id s1))))))

(define random-ip
  (gen-transform
   (lambda (lst)
     (string-join (map number->string lst) "."))
   (with-size 4 (gen-list-of (gen-uint8)))))

(define (random-node)
  (list (random-id) (<- random-ip) (<- (gen-uint16))))

(test-group "insert some nodes"
  (let ((nodes
         '((#${31a64cbc4c8bd1873384b73a6227bec1af8affbe} "244.16.185.97" 8944)
           (#${7310b57e44e50d4b723a781db37687cf8c5acdad} "117.4.153.3" 26269)
           (#${73dbdd7f56965843fe949bc7078315f4468cbc0d} "96.198.18.86" 35258)
           (#${43febec5924b871470d9c7f6df0208f0bd6fb157} "54.169.223.59" 45950)
           (#${436f521b9716392b8bffec75cf5a99f968dca503} "191.67.125.244" 11128)))
        (target-id #${e9acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (lambda (node)
                   (apply (cut server-add-node server <> <> <>) node))
                 nodes)
       (let ((server-node-ids (map node-id (server-all-nodes server))))
         (test 5 (length server-node-ids))
         (test-assert (every (cut member <> server-node-ids blob=?)
                             (map car nodes))))))))

(define (format-nodes nodes)
  (map (lambda (node)
         (list (node-id node) (node-ip node) (node-port node)))
       nodes))

(test-group "get k closest nodes across buckets with == k nodes total"
  (let ((nodes
         '((#${31a64cbc4c8bd1873384b73a6227bec1af8affbe} "244.16.185.97" 8944)
           (#${7310b57e44e50d4b723a781db37687cf8c5acdad} "117.4.153.3" 26269)
           (#${73dbdd7f56965843fe949bc7078315f4468cbc0d} "96.198.18.86" 35258)
           (#${43febec5924b871470d9c7f6df0208f0bd6fb157} "54.169.223.59" 45950)
           (#${436f521b9716392b8bffec75cf5a99f968dca503} "191.67.125.244" 11128)))
        (target-id #${e9acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (lambda (node)
                   (apply (cut server-add-node server <> <> <>) node))
                 nodes)
       (parameterize
           ((max-bucket-size 5))
         (with-store
          (server-env server)
          (lambda (store)
            (test (sort nodes
                        (lambda (a b)
                          (u8vector<?
                           (distance (car a) target-id)
                           (distance (car b) target-id))))
                  (format-nodes
                   (n-closest-nodes store
                                    target-id
                                    (max-bucket-size)))))))))))

(test-group "get k closest nodes across buckets with < k nodes total"
  (let ((nodes
         '((#${31a64cbc4c8bd1873384b73a6227bec1af8affbe} "244.16.185.97" 8944)
           (#${7310b57e44e50d4b723a781db37687cf8c5acdad} "117.4.153.3" 26269)
           (#${73dbdd7f56965843fe949bc7078315f4468cbc0d} "96.198.18.86" 35258)
           (#${43febec5924b871470d9c7f6df0208f0bd6fb157} "54.169.223.59" 45950)
           (#${436f521b9716392b8bffec75cf5a99f968dca503} "191.67.125.244" 11128)))
        (target-id #${e9acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (lambda (node)
                   (apply (cut server-add-node server <> <> <>) node))
                 nodes)
       (parameterize
           ((max-bucket-size 20))
         (with-store
          (server-env server)
          (lambda (store)
            (test (sort nodes
                        (lambda (a b)
                          (u8vector<?
                           (distance (car a) target-id)
                           (distance (car b) target-id))))
                  (format-nodes
                   (n-closest-nodes store
                                    target-id
                                    (max-bucket-size)))))))))))

(test-group "get k closest nodes across buckets with > k nodes total"
  (let ((nodes
         ;; anything > ${80...} starts with 1 bit
         ;; anything < ${80...} and > ${40...} starts with 01 bits
         ;; anything < ${40...} starts with 00 bits
         
         ;; each of these prefixes should have < k nodes to ensure the
         ;; test runs with nodes in 00, 01, and 1 buckets and requires
         ;; accessing at least two buckets. there should be > k nodes
         ;; total < ${80...} to ensure split of 00 and 01 buckets.
         '(
           ;; 00 bucket
           (#${00a64cbc4c8bd1873384b73a6227bec1af8affbe} "244.16.185.97" 8944)
           (#${0010b57e44e50d4b723a781db37687cf8c5acdad} "117.4.153.3" 26269)
           (#${00dbdd7f56965843fe949bc7078315f4468cbc0d} "96.198.18.86" 35258)
           (#${00febec5924b871470d9c7f6df0208f0bd6fb157} "54.169.223.59" 45950)
           (#${006f521b9716392b8bffec75cf5a99f968dca503} "191.67.125.244" 11128)
           (#${00914fe632ac5a91a208d9ec45e535a6dc6778e3} "237.77.12.79" 37926)
           (#${002a811cb441fd8e8fe77c2e4845f4497a7a429b} "33.140.39.223" 14902)
           (#${007532eac08a224313a370b30bffdcc0bd523240} "160.69.251.22" 55563)
           (#${0048d27ba00c0cae4f66929337c68b35b4414a43} "9.201.11.121" 4570)
           (#${0026bd09c75ee639964fdceb978244167bc2ec47} "39.251.134.81" 10873)
           (#${0029b42199e78f64e9b4bcde01ada616508181d7} "136.255.159.33" 27258)
           (#${0003bddd8861dbfa65d9125c87938aaeb4328bf3} "103.14.184.116" 24287)

           ;; 01 bucket
           (#${50fcdf2938e5513296b964e9a9c05d95778b2bea} "76.149.235.115" 29976)
           (#${50f409d151e77dca0158f4eddd33295a76f04a76} "37.19.186.33" 10960)
           (#${5005aee0356992f271472a7d3f888940f16ef057} "147.42.244.158" 42006)
           (#${5090feaeae86ba817323fb47c949ecec6c60fc19} "6.198.159.141" 6195)
           (#${50841b18fc1de328b2152935ec87c24b627da081} "201.21.23.104" 14081)
           (#${506ba8edd6ee48be1f11c9f1b1a8f360955c4576} "130.208.69.58" 17565)
           (#${508465755e8e4d68f3e6dcc5c170cbf928d529d3} "164.134.218.144" 64053)
           (#${5071e12078020178a9d92579c030282f8c35f1b4} "79.182.14.9" 55120)
           (#${506d238729fbbab7a953b7a3c9c13d1f88291483} "56.157.1.45" 15328)
           (#${50ecb51e360d2d3d4b41cb6e451f24a6f382a453} "166.51.2.69" 49359)
           (#${500744ad48364276a2f1c002c59a9f5020fa5486} "26.15.214.50" 30671)
           (#${50e042e4e39a15fc5135dc25840e25f8a37b3f74} "13.180.72.83" 61229)
           (#${50aee946f8b3a1443a3be6917b03ea26ab695b06} "140.248.118.103" 34982)

           ;; 0 bucket
           (#${ff791cdcf7340cc24604172627746ba5618bd88a} "112.182.62.126" 49294)
           (#${ff1540c8e1fbd756f18ce34ee7ece058cbcd73e3} "22.183.93.23" 58566)
           (#${ffb7e589c77df16bf651adfd45a56f7b35cdccef} "153.190.24.155" 862)
           (#${ffbe31cf87f792043211e932e592cf8ad40b7ae3} "217.182.18.175" 59509)
           (#${ff210ac93d462c59ce609aa216b79e99702146a4} "138.189.157.210" 4348)
           ))
        (target-id #${3a0a47cae824ecc72dbc22d218a2b6b73ddf8356}))
    (with-test-servers
     `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (lambda (node)
                   (apply (cut server-add-node server <> <> <>) node))
                 nodes)
       (parameterize
           ((max-bucket-size 20))
         (with-store
          (server-env server)
          (lambda (store)
            (test (take
                   (sort nodes
                         (lambda (a b)
                           (u8vector<?
                            (distance (car a) target-id)
                            (distance (car b) target-id))))
                   20)
                  (format-nodes
                   (n-closest-nodes store
                                    target-id
                                    (max-bucket-size)))))))))))

(test-group "get k closest nodes with zero nodes available"
  (let ((target-id #${e9acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (with-store
        (server-env server)
        (lambda (store)
          (test '()
                (format-nodes
                 (n-closest-nodes store
                                  target-id
                                  (max-bucket-size))))))))))

(test-group "get k closest nodes from single bucket"
  (let ((nodes
         '((#${00a64cbc4c8bd1873384b73a6227bec1af8affbe} "244.16.185.97" 8944)
           (#${0010b57e44e50d4b723a781db37687cf8c5acdad} "117.4.153.3" 26269)
           (#${00dbdd7f56965843fe949bc7078315f4468cbc0d} "96.198.18.86" 35258)
           (#${00febec5924b871470d9c7f6df0208f0bd6fb157} "54.169.223.59" 45950)
           (#${006f521b9716392b8bffec75cf5a99f968dca503} "191.67.125.244" 11128)))
        (target-id #${00acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (lambda (node)
                   (apply (cut server-add-node server <> <> <>) node))
                 nodes)
       (parameterize
           ((max-bucket-size 5))
         (with-store
          (server-env server)
          (lambda (store)
            (test (sort nodes
                        (lambda (a b)
                          (u8vector<?
                           (distance (car a) target-id)
                           (distance (car b) target-id))))
                  (format-nodes
                   (n-closest-nodes store
                                    target-id
                                    (max-bucket-size)))))))))))

(test-group "FIND_NODE returns k closest nodes"
  (let ((nodes (<- (with-size 200 (gen-list-of random-node))))
        (target-id (random-id)))
    (with-test-servers
     `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201)
       (,(make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202))
     (lambda (s1 s2)
       (for-each
        (lambda (node)
          (apply server-add-node (cons s1 node)))
        nodes)
       (let ((nodes-by-distance
              (map (lambda (x)
                     (list (node-id (cdr x))
                           (node-ip (cdr x))
                           (node-port (cdr x))))
                   (sort
                    (map (lambda (node)
                           (cons (distance (node-id node) target-id)
                                 node))
                         (server-all-nodes s1))
                    (lambda (a b)
                      (u8vector<? (car a) (car b)))))))
         (test (take nodes-by-distance 20)
               (send-find-node s2 "127.0.0.1" 4201 target-id)))))))

(test-group "using multiple FIND_NODE hops to find a target node"
  ;; a sender that matches any category
  ;; (start-sender catchall-sender
  ;;               (port-sender (current-output-port))
  ;;               (category *))

  (with-test-servers
   `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201)
     (,(make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202)
     (,(make-id 0 0 0 0 0 1 0 0) "127.0.0.1" 4203)
     (,(make-id 0 0 0 0 1 0 0 0) "127.0.0.1" 4204)
     (,(make-id 0 0 0 1 0 0 0 0) "127.0.0.1" 4205)
     (,(make-id 0 0 1 0 0 0 0 0) "127.0.0.1" 4206)
     (,(make-id 0 1 0 0 0 0 0 0) "127.0.0.1" 4207)
     (,(make-id 1 0 0 0 0 0 0 0) "127.0.0.1" 4208))
   (lambda (s1 s2 s3 s4 s5 s6 s7 s8)
     ;; manually link the network instead of doing a join
     (server-add-node s1 (server-id s2) "127.0.0.1" 4202)
     (server-add-node s2 (server-id s3) "127.0.0.1" 4203)
     (server-add-node s2 (server-id s4) "127.0.0.1" 4204)
     (server-add-node s2 (server-id s5) "127.0.0.1" 4205)
     (server-add-node s3 (server-id s4) "127.0.0.1" 4204)
     (server-add-node s4 (server-id s5) "127.0.0.1" 4205)
     (server-add-node s5 (server-id s6) "127.0.0.1" 4206)
     (server-add-node s6 (server-id s7) "127.0.0.1" 4207)
     (server-add-node s7 (server-id s8) "127.0.0.1" 4208)
     (let ((results
            (server-find-node s1 #${80acd7e8509496bb47409bcec8ae83e5bd5363c8})))
       (test (server-id s8) (node-id (car results)))
       (test "127.0.0.1" (node-ip (car results)))
       (test 4208 (node-port (car results)))))))

;; TODO: create network of multiple nodes and try finding specific node making multiple hops
;; TODO: create netowork of multiple nodes and try store/find_value calls across network
;; join mechanism is implicit in the above tests

;; TODO: create procedure to find new unbound port number have
;;       server-start acception port as optional - if defined bind
;;       only to that port (when you've opened that port in your
;;       firewall), otherwise find any unbound random port to use

(test-exit)
