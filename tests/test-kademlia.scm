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

(test-group "get n closest nodes"
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
                   (k-closest-nodes store target-id))))))))))

(test-group "FIND_NODE returns k closest nodes across buckets"
  (start-sender
   catchall-sender
   (port-sender (current-error-port))
   (category (error)))
  
  (test-generative
      ((nodes (with-size 100 (gen-list-of random-node)))
       (target-id random-id))
    (with-test-servers
     `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201)
       (,(make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202))
     (lambda (s1 s2)
       (for-each (cut apply (cut server-add-node s1 <> <> <>) <>)
                 nodes)
       (let ((nodes-by-distance
              (sort nodes
                    (lambda (a b)
                      (u8vector<? (distance (car a) target-id)
                                  (distance (car b) target-id))))))
         (test (length nodes) (length (server-all-nodes s1)))
         (test nodes-by-distance
               ;(take nodes-by-distance 20)
               (send-find-node s2 "127.0.0.1" 4201 target-id)))))))

;; (test-group "join network"
;;   (let ((log-channel (gochan 0)))
;;     ;; intercept log messgages by logging to a zero buffer gochan in the
;;     ;; hope it slows down logging threads on the events we want to run
;;     ;; tests for
;;     (start-sender
;;      catchall-sender
;;      (structured-sender (cut gochan-send log-channel <>))
;;      (output (<structured))
;;      (category *))
    
;;     (with-test-servers
;;      `((,(make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201)
;;        (,(make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202)
;;        (,(make-id 0 0 0 0 0 0 1 1) "127.0.0.1" 4203)
;;        (,(make-id 0 0 0 0 0 1 0 0) "127.0.0.1" 4204))
;;      (lambda (s1 s2 s3 s4)
;;        (test-error "attempting join without knowing any nodes produces error"
;;                    (join-network s1))
;;        (let* ((completed '())
;;               (t (go (let loop ()
;;                        (gochan-select
;;                         ((log-channel -> msg)
;;                          (match msg
;;                            (("join-complete" id)
;;                             (unless (member id completed blob=?)
;;                               (set! completed (cons id completed)))
;;                             (cond
;;                              ((blob=? (server-id s2) id)
;;                               (test `((,(server-id s1) "127.0.0.1" 4201))
;;                                     (format-nodes (server-all-nodes s2))))
;;                              ((blob=? (server-id s3) id)
;;                               (test `((,(server-id s1) "127.0.0.1" 4201)
;;                                       (,(server-id s2) "127.0.0.1" 4202))
;;                                     (format-nodes (server-all-nodes s3))))
;;                              ((blob=? (server-id s4) id)
;;                               (test `((,(server-id s1) "127.0.0.1" 4201)
;;                                       (,(server-id s2) "127.0.0.1" 4202)
;;                                       (,(server-id s3) "127.0.0.1" 4203))
;;                                     (format-nodes (server-all-nodes s4))))))
;;                            (else
;;                             ;; ignore other messages
;;                             #f))
;;                          (loop)))))))
;;          (server-add-node s2 (server-id s1) "127.0.0.1" 4201)
;;          (server-add-node s3 (server-id s1) "127.0.0.1" 4201)
;;          (server-add-node s4 (server-id s2) "127.0.0.1" 4202)
;;          (thread-join! (go (join-network s2)) 4)
;;          (thread-join! (go (join-network s3)) 4)
;;          (thread-join! (go (join-network s4)) 4)
;;          (thread-terminate! t)
;;          (test 3 (length completed))
;;          (test `((,(server-id s2) "127.0.0.1" 4202)
;;                  (,(server-id s3) "127.0.0.1" 4203)
;;                  (,(server-id s4) "127.0.0.1" 4204))
;;                (format-nodes (server-all-nodes s1)))
;;          )))))

;; TODO: create procedure to find new unbound port number have
;;       server-start acception port as optional - if defined bind
;;       only to that port (when you've opened that port in your
;;       firewall), otherwise find any unbound random port to use

(test-exit)
