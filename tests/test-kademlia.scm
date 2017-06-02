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

 
(start-sender
 catchall-sender
 (port-sender (current-error-port))
 (category error))

;; (start-sender
;;  catchall-sender
;;  (port-sender (current-error-port))
;;  (category *))

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

(test-group "routing-entry->blob and blob->routing-entry"
  (let ((entry (make-routing-entry
               node: (make-node (u8vector->blob/shared (make-u8vector 20 0))
                                "192.168.0.101"
                                4219)
               first-seen: 100
               last-seen: 456
               failed-requests: 0)))
    (test-assert (blob? (routing-entry->blob entry)))
    (test entry (blob->routing-entry (routing-entry->blob entry)))))

(test-group "bucket-insert"
  (let ((entry1 (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 101
                 last-seen: 456
                 failed-requests: 0))
        (entry2 (make-routing-entry
                 node: (make-node #${2000000000000000000000000000000000000000}
                                  "192.168.0.102"
                                  14220)
                 first-seen: 102
                 last-seen: 456
                 failed-requests: 0))
        (entry3 (make-routing-entry
                 node: (make-node #${8000000000000000000000000000000000000000}
                                  "192.168.0.103"
                                  8000)
                 first-seen: 103
                 last-seen: 456
                 failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0 0)) entry1)
       (bucket-insert store (list->bitstring '(0 0)) entry2)
       (bucket-insert store (list->bitstring '(1)) entry3)
       (test (list entry1 entry2)
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0 0)))))
       (test (list entry3)
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(1)))))))))

(test-group "bucket-remove"
  (let ((entry1 (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 101
                 last-seen: 456
                 failed-requests: 0))
        (entry2 (make-routing-entry
                 node: (make-node #${2000000000000000000000000000000000000000}
                                  "192.168.0.102"
                                  14220)
                 first-seen: 102
                 last-seen: 456
                 failed-requests: 0))
        (entry3 (make-routing-entry
                 node: (make-node #${4000000000000000000000000000000000000000}
                                  "192.168.0.103"
                                  8000)
                 first-seen: 103
                 last-seen: 456
                 failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0)) entry1)
       (bucket-insert store (list->bitstring '(0)) entry2)
       (bucket-insert store (list->bitstring '(0)) entry3)
       (bucket-remove store (list->bitstring '(0)) entry2)
       (test (list entry1 entry3)
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0)))))))))

(test-group "bucket-destroy"
  (let ((entry1 (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 101
                 last-seen: 456
                 failed-requests: 0))
        (entry2 (make-routing-entry
                 node: (make-node #${2000000000000000000000000000000000000000}
                                  "192.168.0.102"
                                  14220)
                 first-seen: 102
                 last-seen: 456
                 failed-requests: 0))
        (entry3 (make-routing-entry
                 node: (make-node #${8000000000000000000000000000000000000000}
                                  "192.168.0.103"
                                  8000)
                 first-seen: 103
                 last-seen: 456
                 failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0 0)) entry1)
       (bucket-insert store (list->bitstring '(0 0)) entry2)
       (bucket-insert store (list->bitstring '(1)) entry3)
       (bucket-destroy store (list->bitstring '(0 0)))
       (test '()
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0 0)))))
       (test (list entry3)
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(1)))))))))

(test-group "bucket-split"
  (let ((entry1 (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 101
                 last-seen: 456
                 failed-requests: 0))
        (entry2 (make-routing-entry
                 node: (make-node #${2000000000000000000000000000000000000000}
                                  "192.168.0.102"
                                  14220)
                 first-seen: 102
                 last-seen: 456
                 failed-requests: 0))
        (entry3 (make-routing-entry
                 node: (make-node #${4000000000000000000000000000000000000000}
                                  "192.168.0.103"
                                  8000)
                 first-seen: 103
                 last-seen: 456
                 failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0)) entry1)
       (bucket-insert store (list->bitstring '(0)) entry2)
       (bucket-insert store (list->bitstring '(0)) entry3)
       (bucket-split store (list->bitstring '(0)))
       (test (list entry1 entry2)
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0 0)))))
       (test (list entry3)
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0 1)))))
       (test '()
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0)))))))))

(test-group "bucket-join"
  (let ((entry1 (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 101
                 last-seen: 456
                 failed-requests: 0))
        (entry2 (make-routing-entry
                 node: (make-node #${2000000000000000000000000000000000000000}
                                  "192.168.0.102"
                                  14220)
                 first-seen: 102
                 last-seen: 456
                 failed-requests: 0))
        (entry3 (make-routing-entry
                 node: (make-node #${4000000000000000000000000000000000000000}
                                  "192.168.0.103"
                                  8000)
                 first-seen: 103
                 last-seen: 456
                 failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0 0)) entry1)
       (bucket-insert store (list->bitstring '(0 0)) entry2)
       (bucket-insert store (list->bitstring '(0 1)) entry3)
       (bucket-join store (list->bitstring '(0)))
       (test '()
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0 0)))))
       (test '()
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0 1)))))
       (test (list entry1 entry2 entry3)
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0)))))))))

(test-group "bucket-size"
  (let ((entry1 (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 101
                 last-seen: 456
                 failed-requests: 0))
        (entry2 (make-routing-entry
                 node: (make-node #${2000000000000000000000000000000000000000}
                                  "192.168.0.102"
                                  14220)
                 first-seen: 102
                 last-seen: 456
                 failed-requests: 0))
        (entry3 (make-routing-entry
                 node: (make-node #${4000000000000000000000000000000000000000}
                                  "192.168.0.103"
                                  8000)
                 first-seen: 103
                 last-seen: 456
                 failed-requests: 0)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0 0)) entry1)
       (bucket-insert store (list->bitstring '(0 0)) entry2)
       (bucket-insert store (list->bitstring '(0 1)) entry3)
       (test 2 (bucket-size store (list->bitstring '(0 0))))
       (test 1 (bucket-size store (list->bitstring '(0 1))))
       (test 0 (bucket-size store (list->bitstring '(1))))))))

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
  (let ((entry1 (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 103
                 last-seen: 456
                 failed-requests: 1))
        (entry2 (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.102"
                                  14220)
                 first-seen: 101
                 last-seen: 456
                 failed-requests: 2)))
    (with-test-store
     (lambda (store)
       (bucket-insert store (list->bitstring '(0)) entry1)
       (bucket-insert store (list->bitstring '(0)) entry2)
       (test (list entry2)
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0)))))))))

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
                    (make-routing-entry
                     node: (make-node #${0000000000000000000000000000000000000000}
                                      "192.168.0.101"
                                      4219)
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
                    (make-routing-entry
                     node: (make-node #${ffffffffffffffffffffffffffffffffffffffff}
                                      "192.168.0.102"
                                      4219)
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
                    (make-routing-entry
                      node: (make-node #${5555555555555555555555555555555555555555}
                                       "192.168.0.103"
                                       4219)
                      first-seen: 100
                      last-seen: 200
                      failed-requests: 0))
     (bucket-insert store
                    (list->bitstring '(0 1 1 1))
                    (make-routing-entry
                      node: (make-node #${7fffffffffffffffffffffffffffffffffffffff}
                                       "192.168.0.103"
                                       4219)
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
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000000})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          (make-node
           #${0000000000000000000000000000000000000000}
           "192.168.0.101"
           4219)))
       (test (list
              (make-routing-entry
               node: (make-node #${0000000000000000000000000000000000000000}
                                "192.168.0.101"
                                4219)
               first-seen: 300
               last-seen: 300
               failed-requests: 0))
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0)))))
       ))))

;; TODO: make sure failed-requests isn't reset too?
(test-group "update-routing-table: insert existing node"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000000})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          (make-node
           #${0000000000000000000000000000000000000000}
           "192.168.0.101"
           4219)))
       (fluid-let ((current-seconds (cut identity 400)))
         (update-routing-table
          store
          (make-node
           #${0000000000000000000000000000000000000000}
           "192.168.0.101"
           4219)))
       (test (list
              (make-routing-entry
               node: (make-node #${0000000000000000000000000000000000000000}
                                "192.168.0.101"
                                4219)
               first-seen: 300
               last-seen: 400
               failed-requests: 0))
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0)))))
       ))))

(test-group "update-routing-table: insert new node into existing bucket"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000000})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          (make-node
           #${0000000000000000000000000000000000000000}
           "192.168.0.101"
           4219)))
       (fluid-let ((current-seconds (cut identity 400)))
         (update-routing-table
          store
          (make-node
           #${5555555555555555555555555555555555555555}
           "192.168.0.102"
           4219)))
       (test (list
              (make-routing-entry
               node: (make-node #${0000000000000000000000000000000000000000}
                                "192.168.0.101"
                                4219)
               first-seen: 300
               last-seen: 300
               failed-requests: 0)
              (make-routing-entry
               node: (make-node #${5555555555555555555555555555555555555555}
                                "192.168.0.102"
                                4219)
               first-seen: 400
               last-seen: 400
               failed-requests: 0))
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0)))))
       ))))

(test-group "update-routing-table: insert into existing bucket - same first-seen"
  ;; nodes with same first-seen values should then sort on node-id
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000000})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          (make-node
           #${0000000000000000000000000000000000000000}
           "192.168.0.101"
           4219))
         (update-routing-table
          store
          (make-node
           #${5555555555555555555555555555555555555555}
           "192.168.0.102"
           4219)))
       (test (list
              (make-routing-entry
               node: (make-node #${0000000000000000000000000000000000000000}
                                "192.168.0.101"
                                4219)
               first-seen: 300
               last-seen: 300
               failed-requests: 0)
              (make-routing-entry
               node: (make-node #${5555555555555555555555555555555555555555}
                                "192.168.0.102"
                                4219)
               first-seen: 300
               last-seen: 300
               failed-requests: 0))
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0)))))
       ))))

(test-group "update-routing-table: insert new node into new bucket"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000000})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       ;;  use a fixed current time for comparisons
       (fluid-let ((current-seconds (cut identity 300)))
         (update-routing-table
          store
          (make-node
           #${0000000000000000000000000000000000000000}
           "192.168.0.101"
           4219)))
       (fluid-let ((current-seconds (cut identity 400)))
         (update-routing-table
          store
          (make-node
           #${ffffffffffffffffffffffffffffffffffffffff}
           "192.168.0.102"
           4219)))
       (test (list
              (make-routing-entry
               node: (make-node #${0000000000000000000000000000000000000000}
                                "192.168.0.101"
                                4219)
               first-seen: 300
               last-seen: 300
               failed-requests: 0))
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(0)))))
       (test (list
              (make-routing-entry
               node: (make-node #${ffffffffffffffffffffffffffffffffffffffff}
                                "192.168.0.102"
                                4219)
               first-seen: 400
               last-seen: 400
               failed-requests: 0))
             (lazy-seq->list
              (bucket-entries store (list->bitstring '(1)))))
       ))))

(test-group "update-routing-table: split bucket"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000000})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       (parameterize
           ;; use a small 'k' parameter (bucket size)
           ((max-bucket-size 3))
         ;;  use a fixed current time for comparisons
         (fluid-let ((current-seconds (cut identity 300)))
           (update-routing-table
            store
            (make-node
             #${ffffffffffffffffffffffffffffffffffffffff} ; 11111111
             "192.168.0.101"
             4219))
           (update-routing-table
            store
            (make-node
             #${cccccccccccccccccccccccccccccccccccccccc} ; 11001100
             "192.168.0.102"
             4219))
           (update-routing-table
            store
            (make-node
             #${bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb} ; 10111011
             "192.168.0.103"
             4219))
           (update-routing-table
            store
            (make-node
             #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa} ; 10101010
             "192.168.0.104"
             4219)))
         (test '((1 0) (1 1))
               (map (compose bitstring->list blob->prefix)
                    (lazy-seq->list
                     (keys (store-txn store) (store-routing-table store)))))
         (test '(#${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}
                 #${bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}
                 #${cccccccccccccccccccccccccccccccccccccccc}
                 #${ffffffffffffffffffffffffffffffffffffffff})
               (map (compose node-id routing-entry-node blob->routing-entry)
                    (lazy-seq->list
                     (all-values (store-txn store)
                                 (store-routing-table store)))))
         (test '()
               (lazy-seq->list
                (bucket-entries store (list->bitstring '(0)))))
         (test (list
                (make-routing-entry
                 node: (make-node #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa}
                                  "192.168.0.104"
                                  4219)
                 first-seen: 300
                 last-seen: 300
                 failed-requests: 0)
                (make-routing-entry
                 node: (make-node #${bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb}
                                  "192.168.0.103"
                                  4219)
                 first-seen: 300
                 last-seen: 300
                 failed-requests: 0))
               (lazy-seq->list
                (bucket-entries store (list->bitstring '(1 0)))))
         (test (list
                (make-routing-entry
                 node: (make-node #${cccccccccccccccccccccccccccccccccccccccc}
                                  "192.168.0.102"
                                  4219)
                 first-seen: 300
                 last-seen: 300
                 failed-requests: 0)
                (make-routing-entry
                 node: (make-node #${ffffffffffffffffffffffffffffffffffffffff}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 300
                 last-seen: 300
                 failed-requests: 0))
               (lazy-seq->list
                (bucket-entries store (list->bitstring '(1 1)))))
         (test '()
               (lazy-seq->list
                (bucket-entries store (list->bitstring '(1)))))
         )))))

(test-group "update-routing-table: don't split bucket"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000000})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       (parameterize
           ;; use a small 'k' parameter (bucket size)
           ((max-bucket-size 3))
         ;;  use a fixed current time for comparisons
         (fluid-let ((current-seconds (cut identity 300)))
           (update-routing-table
            store
            (make-node
             #${0000000000000000000000000000000000000000} ; 00000000
             "192.168.0.101"
             4219)))
         (fluid-let ((current-seconds (cut identity 400)))
           (update-routing-table
            store
            (make-node
             #${3333333333333333333333333333333333333333} ; 00110011
             "192.168.0.102"
             4219)))
         (fluid-let ((current-seconds (cut identity 500)))
           (update-routing-table
            store
            (make-node
             #${4444444444444444444444444444444444444444} ; 01000100
             "192.168.0.103"
             4219)))
         (fluid-let ((current-seconds (cut identity 600)))
           (update-routing-table
            store
            (make-node
             #${5555555555555555555555555555555555555555} ; 01010101
             "192.168.0.104"
             4219)))
         (test "new entry is discarded"
               (list
                (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 300
                 last-seen: 300
                 failed-requests: 0)
                (make-routing-entry
                 node: (make-node #${3333333333333333333333333333333333333333}
                                  "192.168.0.102"
                                  4219)
                 first-seen: 400
                 last-seen: 400
                 failed-requests: 0)
                (make-routing-entry
                 node: (make-node #${4444444444444444444444444444444444444444}
                                  "192.168.0.103"
                                  4219)
                 first-seen: 500
                 last-seen: 500
                 failed-requests: 0))
               (lazy-seq->list
                (bucket-entries store (list->bitstring '(0)))))
         (test '()
               (lazy-seq->list
                (bucket-entries store (list->bitstring '(1)))))
         )))))

(test-group "update-routing-table: inserting existing node into full bucket updates last-seen"
  (with-test-store
   (lambda (store)
     (let ((test-id #${aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa00000000})) ; 101010...
       ;; use a fixed ID so bucket splits are predictable
       (local-id-set! store test-id)
       (parameterize
           ;; use a small 'k' parameter (bucket size)
           ((max-bucket-size 3))
         ;;  use a fixed current time for comparisons
         (fluid-let ((current-seconds (cut identity 300)))
           (update-routing-table
            store
            (make-node
             #${0000000000000000000000000000000000000000} ; 00000000
             "192.168.0.101"
             4219)))
         (fluid-let ((current-seconds (cut identity 400)))
           (update-routing-table
            store
            (make-node
             #${3333333333333333333333333333333333333333} ; 00110011
             "192.168.0.102"
             4219)))
         (fluid-let ((current-seconds (cut identity 500)))
           (update-routing-table
            store
            (make-node
             #${4444444444444444444444444444444444444444} ; 01000100
             "192.168.0.103"
             4219)))
         (fluid-let ((current-seconds (cut identity 600)))
           (update-routing-table
            store
            (make-node
             #${0000000000000000000000000000000000000000} ; 01010101
             "192.168.0.104"
             4219)))
         (test "new entry is discarded"
               (list
                (make-routing-entry
                 node: (make-node #${0000000000000000000000000000000000000000}
                                  "192.168.0.101"
                                  4219)
                 first-seen: 300
                 last-seen: 600
                 failed-requests: 0)
                (make-routing-entry
                 node: (make-node #${3333333333333333333333333333333333333333}
                                  "192.168.0.102"
                                  4219)
                 first-seen: 400
                 last-seen: 400
                 failed-requests: 0)
                (make-routing-entry
                 node: (make-node #${4444444444444444444444444444444444444444}
                                  "192.168.0.103"
                                  4219)
                 first-seen: 500
                 last-seen: 500
                 failed-requests: 0))
               (lazy-seq->list
                (bucket-entries store (list->bitstring '(0)))))
         (test '()
               (lazy-seq->list
                (bucket-entries store (list->bitstring '(1)))))
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
      (let ((node (car configs)))
        (let ((db-path (make-pathname "tests/server-dbs/"
                                      (bin->hex (node-id node)))))
          (clear-testdb db-path)
          (with-server
           db-path
           (node-ip node)
           (node-port node)
           (lambda (server)
             (server-id-set! server (node-id node))
             (with-test-servers
              (cdr configs) thunk (cons server servers))))))))

(test-group "PING"
  (with-test-servers
   (list
    (make-node #${0000000000000000000000000000000000000000} "127.0.0.1" 4218)
    (make-node #${ffffffffffffffffffffffffffffffffffffffff} "127.0.0.1" 4217))
   (lambda (server1 server2)
     (test #${ffffffffffffffffffffffffffffffffffffffff}
           (send-ping server1 "127.0.0.1" 4217))
     (test #${0000000000000000000000000000000000000000}
           (send-ping server2 "127.0.0.1" 4218)))))

(test-group "simple STORE and FIND_VALUE"
  (let ((hashed-key1 (generic-hash (string->blob "key-one") size: 20))
        (hashed-key2 (generic-hash (string->blob "key-two") size: 20))
        (val1 (string->blob "value-one"))
        (val2 (string->blob "value-two")))
    (with-test-servers
     (list
      (make-node #${0000000000000000000000000000000000000000} "127.0.0.1" 4218)
      (make-node #${ffffffffffffffffffffffffffffffffffffffff} "127.0.0.1" 4217))
     (lambda (server1 server2)
       (send-store server2 "127.0.0.1" 4217 hashed-key1 val1)
       (send-store server1 "127.0.0.1" 4218 hashed-key2 val2)
       (test `((value . ,val1))
             (send-find-value server2 "127.0.0.1" 4217 hashed-key1))
       (test `((value . ,val2))
             (send-find-value server1 "127.0.0.1" 4218 hashed-key2))))))

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
  (let ((test-nodes
         (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201)
               (make-node (make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202)
               (make-node (make-id 0 0 0 0 0 1 0 0) "127.0.0.1" 4203)
               (make-node (make-id 0 0 0 0 1 0 0 0) "127.0.0.1" 4204)
               (make-node (make-id 0 0 0 1 0 0 0 0) "127.0.0.1" 4205)
               (make-node (make-id 0 0 1 0 0 0 0 0) "127.0.0.1" 4206)
               (make-node (make-id 0 1 0 0 0 0 0 0) "127.0.0.1" 4207)
               (make-node (make-id 1 0 0 0 0 0 0 0) "127.0.0.1" 4208))))
    (with-test-servers
     (take test-nodes 2)
     (lambda (s1 s2)
       (receive (n1 n2 n3 n4 n5 n6 n7 n8) (apply values test-nodes)
         (server-add-node s1 n2)
         (server-add-node s1 n3)
         (server-add-node s1 n4)
         (server-add-node s1 n5)
         (server-add-node s1 n6)
         (server-add-node s1 n7)
         (server-add-node s1 n8)
         (test `((nodes . ,(cdr test-nodes)))
               (send-find-node s2 "127.0.0.1" 4201 (server-id s1))))))))

(define random-ip
  (gen-transform
   (lambda (lst)
     (string-join (map number->string lst) "."))
   (with-size 4 (gen-list-of (gen-uint8)))))

(define (random-node)
  (make-node (random-id) (<- random-ip) (<- (gen-uint16))))

(test-group "insert some nodes"
  (let ((nodes
         (list
          (make-node #${31a64cbc4c8bd1873384b73a6227bec1af8affbe}
                     "244.16.185.97"
                     8944)
          (make-node #${7310b57e44e50d4b723a781db37687cf8c5acdad}
                     "117.4.153.3"
                     26269)
          (make-node #${73dbdd7f56965843fe949bc7078315f4468cbc0d}
                     "96.198.18.86"
                     35258)
          (make-node #${43febec5924b871470d9c7f6df0208f0bd6fb157}
                     "54.169.223.59"
                     45950)
          (make-node #${436f521b9716392b8bffec75cf5a99f968dca503}
                     "191.67.125.244"
                     11128)))
        (target-id #${e9acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (cut server-add-node server <>) nodes)
       (let ((server-node-ids (map node-id (server-all-nodes server))))
         (test 5 (length server-node-ids))
         (test-assert (every (cut member <> server-node-ids blob=?)
                             (map node-id nodes))))))))

(test-group "get k closest nodes across buckets with == k nodes total"
  (let ((nodes
         (list
          (make-node #${31a64cbc4c8bd1873384b73a6227bec1af8affbe}
                     "244.16.185.97"
                     8944)
          (make-node #${7310b57e44e50d4b723a781db37687cf8c5acdad}
                     "117.4.153.3"
                     26269)
          (make-node #${73dbdd7f56965843fe949bc7078315f4468cbc0d}
                     "96.198.18.86"
                     35258)
          (make-node #${43febec5924b871470d9c7f6df0208f0bd6fb157}
                     "54.169.223.59"
                     45950)
          (make-node #${436f521b9716392b8bffec75cf5a99f968dca503}
                     "191.67.125.244"
                     11128)))
        (target-id #${e9acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (cut server-add-node server <>) nodes)
       (parameterize
           ((max-bucket-size 5))
         (with-store
          (server-env server)
          (lambda (store)
            (test (sort nodes
                        (lambda (a b)
                          (u8vector<?
                           (distance (node-id a) target-id)
                           (distance (node-id b) target-id))))
                  (n-closest-nodes store
                                   target-id
                                   (max-bucket-size))))))))))

(test-group "get k closest nodes across buckets with < k nodes total"
  (let ((nodes
         (list
          (make-node #${31a64cbc4c8bd1873384b73a6227bec1af8affbe}
                     "244.16.185.97"
                     8944)
          (make-node #${7310b57e44e50d4b723a781db37687cf8c5acdad}
                     "117.4.153.3"
                     26269)
          (make-node #${73dbdd7f56965843fe949bc7078315f4468cbc0d}
                     "96.198.18.86"
                     35258)
          (make-node #${43febec5924b871470d9c7f6df0208f0bd6fb157}
                     "54.169.223.59"
                     45950)
          (make-node #${436f521b9716392b8bffec75cf5a99f968dca503}
                     "191.67.125.244"
                     11128)))
        (target-id #${e9acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (cut server-add-node server <>) nodes)
       (parameterize
           ((max-bucket-size 20))
         (with-store
          (server-env server)
          (lambda (store)
            (test (sort nodes
                        (lambda (a b)
                          (u8vector<?
                           (distance (node-id a) target-id)
                           (distance (node-id b) target-id))))
                  (n-closest-nodes store
                                   target-id
                                   (max-bucket-size))))))))))

(test-group "get k closest nodes across buckets with > k nodes total"
  (let ((nodes
         ;; anything > ${80...} starts with 1 bit
         ;; anything < ${80...} and > ${40...} starts with 01 bits
         ;; anything < ${40...} starts with 00 bits
         
         ;; each of these prefixes should have < k nodes to ensure the
         ;; test runs with nodes in 00, 01, and 1 buckets and requires
         ;; accessing at least two buckets. there should be > k nodes
         ;; total < ${80...} to ensure split of 00 and 01 buckets.
         (list
          ;; 00 bucket
          (make-node #${00a64cbc4c8bd1873384b73a6227bec1af8affbe}
                     "244.16.185.97"
                     8944)
          (make-node #${0010b57e44e50d4b723a781db37687cf8c5acdad}
                     "117.4.153.3"
                     26269)
          (make-node #${00dbdd7f56965843fe949bc7078315f4468cbc0d}
                     "96.198.18.86"
                     35258)
          (make-node #${00febec5924b871470d9c7f6df0208f0bd6fb157}
                     "54.169.223.59"
                     45950)
          (make-node #${006f521b9716392b8bffec75cf5a99f968dca503}
                     "191.67.125.244"
                     11128)
          (make-node #${00914fe632ac5a91a208d9ec45e535a6dc6778e3}
                     "237.77.12.79"
                     37926)
          (make-node #${002a811cb441fd8e8fe77c2e4845f4497a7a429b}
                     "33.140.39.223"
                     14902)
          (make-node #${007532eac08a224313a370b30bffdcc0bd523240}
                     "160.69.251.22"
                     55563)
          (make-node #${0048d27ba00c0cae4f66929337c68b35b4414a43}
                     "9.201.11.121"
                     4570)
          (make-node #${0026bd09c75ee639964fdceb978244167bc2ec47}
                     "39.251.134.81"
                     10873)
          (make-node #${0029b42199e78f64e9b4bcde01ada616508181d7}
                     "136.255.159.33"
                     27258)
          (make-node #${0003bddd8861dbfa65d9125c87938aaeb4328bf3}
                     "103.14.184.116"
                     24287)
          ;; 01 bucket
          (make-node #${50fcdf2938e5513296b964e9a9c05d95778b2bea}
                     "76.149.235.115"
                     29976)
          (make-node #${50f409d151e77dca0158f4eddd33295a76f04a76}
                     "37.19.186.33"
                     10960)
          (make-node #${5005aee0356992f271472a7d3f888940f16ef057}
                     "147.42.244.158"
                     42006)
          (make-node #${5090feaeae86ba817323fb47c949ecec6c60fc19}
                     "6.198.159.141"
                     6195)
          (make-node #${50841b18fc1de328b2152935ec87c24b627da081}
                     "201.21.23.104"
                     14081)
          (make-node #${506ba8edd6ee48be1f11c9f1b1a8f360955c4576}
                     "130.208.69.58"
                     17565)
          (make-node #${508465755e8e4d68f3e6dcc5c170cbf928d529d3}
                     "164.134.218.144"
                     64053)
          (make-node #${5071e12078020178a9d92579c030282f8c35f1b4}
                     "79.182.14.9"
                     55120)
          (make-node #${506d238729fbbab7a953b7a3c9c13d1f88291483}
                     "56.157.1.45"
                     15328)
          (make-node #${50ecb51e360d2d3d4b41cb6e451f24a6f382a453}
                     "166.51.2.69"
                     49359)
          (make-node #${500744ad48364276a2f1c002c59a9f5020fa5486}
                     "26.15.214.50"
                     30671)
          (make-node #${50e042e4e39a15fc5135dc25840e25f8a37b3f74}
                     "13.180.72.83"
                     61229)
          (make-node #${50aee946f8b3a1443a3be6917b03ea26ab695b06}
                     "140.248.118.103"
                     34982)
          ;; 0 bucket
          (make-node #${ff791cdcf7340cc24604172627746ba5618bd88a}
                     "112.182.62.126"
                     49294)
          (make-node #${ff1540c8e1fbd756f18ce34ee7ece058cbcd73e3}
                     "22.183.93.23"
                     58566)
          (make-node #${ffb7e589c77df16bf651adfd45a56f7b35cdccef}
                     "153.190.24.155"
                     862)
          (make-node #${ffbe31cf87f792043211e932e592cf8ad40b7ae3}
                     "217.182.18.175"
                     59509)
          (make-node #${ff210ac93d462c59ce609aa216b79e99702146a4}
                     "138.189.157.210"
                     4348)
          ))
        (target-id #${3a0a47cae824ecc72dbc22d218a2b6b73ddf8356}))
    (with-test-servers
     (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (cut server-add-node server <>) nodes)
       (parameterize
           ((max-bucket-size 20))
         (with-store
          (server-env server)
          (lambda (store)
            (test (take
                   (sort nodes
                         (lambda (a b)
                           (u8vector<?
                            (distance (node-id a) target-id)
                            (distance (node-id b) target-id))))
                   20)
                  (n-closest-nodes store
                                   target-id
                                   (max-bucket-size))))))))))

(test-group "get k closest nodes with zero nodes available"
  (let ((target-id #${e9acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (with-store
        (server-env server)
        (lambda (store)
          (test '()
                (n-closest-nodes store
                                 target-id
                                 (max-bucket-size)))))))))

(test-group "get k closest nodes from single bucket"
  (let ((nodes
         (list
          (make-node #${00a64cbc4c8bd1873384b73a6227bec1af8affbe}
                     "244.16.185.97"
                     8944)
          (make-node #${0010b57e44e50d4b723a781db37687cf8c5acdad}
                     "117.4.153.3"
                     26269)
          (make-node #${00dbdd7f56965843fe949bc7078315f4468cbc0d}
                     "96.198.18.86"
                     35258)
          (make-node #${00febec5924b871470d9c7f6df0208f0bd6fb157}
                     "54.169.223.59"
                     45950)
          (make-node #${006f521b9716392b8bffec75cf5a99f968dca503}
                     "191.67.125.244"
                     11128)))
        (target-id #${00acd7e8509496bb47409bcec8ae83e5bd5363c8}))
    (with-test-servers
     (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201))
     (lambda (server)
       (for-each (cut server-add-node server <>) nodes)
       (parameterize
           ((max-bucket-size 5))
         (with-store
          (server-env server)
          (lambda (store)
            (test (sort nodes
                        (lambda (a b)
                          (u8vector<?
                           (distance (node-id a) target-id)
                           (distance (node-id b) target-id))))
                  (n-closest-nodes store
                                   target-id
                                   (max-bucket-size))))))))))

(test-group "FIND_NODE returns k closest nodes"
  (let ((random-nodes (<- (with-size 200 (gen-list-of random-node))))
        (target-id (random-id))
        (test-nodes
         (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201)
               (make-node (make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202))))
    (with-test-servers
     test-nodes
     (lambda (s1 s2)
       (let ((nodes (cons (second test-nodes) random-nodes)))
         (for-each (cut server-add-node s1 <>) nodes)
         (let ((nodes-by-distance
                (map cdr
                     (sort
                      (map (lambda (node)
                             (cons (distance (node-id node) target-id)
                                   node))
                           (server-all-nodes s1))
                      (lambda (a b)
                        (u8vector<? (car a) (car b)))))))
           (test `((nodes . ,(take nodes-by-distance 20)))
                 (send-find-node s2 "127.0.0.1" 4201 target-id))))))))

(test-group "using multiple FIND_NODE hops to find a target node"
  (let ((test-nodes
         (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201)
               (make-node (make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202)
               (make-node (make-id 0 0 0 0 0 1 0 0) "127.0.0.1" 4203)
               (make-node (make-id 0 0 0 0 1 0 0 0) "127.0.0.1" 4204)
               (make-node (make-id 0 0 0 1 0 0 0 0) "127.0.0.1" 4205)
               (make-node (make-id 0 0 1 0 0 0 0 0) "127.0.0.1" 4206)
               (make-node (make-id 0 1 0 0 0 0 0 0) "127.0.0.1" 4207)
               (make-node (make-id 1 0 0 0 0 0 0 0) "127.0.0.1" 4208))))
    (with-test-servers
     test-nodes
     (lambda (s1 s2 s3 s4 s5 s6 s7 s8)
       (receive (n1 n2 n3 n4 n5 n6 n7 n8) (apply values test-nodes)
         ;; manually link the network instead of doing a join as with a
         ;; small network all nodes would have total knowledge of their
         ;; peers and multiple hops would not be necessary
         (server-add-node s1 n2)
         (server-add-node s2 n3)
         (server-add-node s2 n4)
         (server-add-node s2 n5)
         (server-add-node s3 n4)
         (server-add-node s4 n5)
         (server-add-node s5 n6)
         (server-add-node s6 n7)
         (server-add-node s7 n8)
         (let* ((results (server-find-node s1 (server-id s8)))
                (closest (car results)))
           (test n8 (car results))))))))

(test-group "network of < k nodes, each should have total knowledge of network"
  (let ((test-nodes
         (list (make-node (make-id 0 0 0 0 0 0 0 1) "127.0.0.1" 4201)
               (make-node (make-id 0 0 0 0 0 0 1 0) "127.0.0.1" 4202)
               (make-node (make-id 0 0 0 0 0 1 0 0) "127.0.0.1" 4203)
               (make-node (make-id 0 0 0 0 1 0 0 0) "127.0.0.1" 4204)
               (make-node (make-id 0 0 0 1 0 0 0 0) "127.0.0.1" 4205)
               (make-node (make-id 0 0 1 0 0 0 0 0) "127.0.0.1" 4206)
               (make-node (make-id 0 1 0 0 0 0 0 0) "127.0.0.1" 4207)
               (make-node (make-id 1 0 0 0 0 0 0 0) "127.0.0.1" 4208))))
    (with-test-servers
     test-nodes
     (lambda servers
       (for-each
        (lambda (server)
          ;; each node needs to know of at least one other before joining
          (server-add-node server (car test-nodes))
          (server-join-network server))
        (cdr servers))
       (for-each
        (lambda (s)
          (test (sprintf "routing table for ~A" (server-id s))
                (filter (lambda (x)
                          (not (blob=? (node-id x) (server-id s))))
                        test-nodes)
                (server-all-nodes s)))
        servers)))))

(define (gen-local-nodes n)
  (lambda ()
    (list-tabulate
     n
     (lambda (i)
       (make-node (random-id) "127.0.0.1" (+ 4201 i))))))

(define (random-key)
  ;(random-blob (+ 1 (random 200))))
  (random-blob (+ 1 (random 10))))

(define (random-value)
  ;(random-blob (+ 1 (random 1000))))
  (random-blob (+ 1 (random 100))))

(define (shuffle l)
  (let ((len (length l)))
    (map cdr
         (sort! (map (lambda (x) (cons (random len) x)) l)
                (lambda (x y) (< (car x) (car y)))))))

(test-group "store and retrieve values using random stable network"
  (parameterize
      ((current-test-generative-iterations 1))
    (let ((number-of-nodes 20)
          (number-of-keys 10))
      (test-generative
          ((nodes (gen-local-nodes number-of-nodes))
           ;; TODO: test for duplicates?
           (data (with-size number-of-keys
                            (gen-list-of
                             (gen-pair-of random-key random-value))))
           (store-order
            (lambda ()
              (zip (<- (gen-list-of (gen-fixnum 0 (- number-of-nodes 1))
                                    number-of-keys))
                   (shuffle (list-tabulate number-of-keys identity)))))
           (find-order
            (lambda ()
              (zip (<- (gen-list-of (gen-fixnum 0 (- number-of-nodes 1))
                                    number-of-keys))
                   (shuffle (list-tabulate number-of-keys identity))))))
        (with-test-servers
         nodes
         (lambda servers
           (for-each
            (lambda (server)
              ;; each node needs to know of at least one other before joining
              (server-add-node server (car nodes))
              (server-join-network server))
            (cdr servers))
           ;; store values
           (for-each
            (match-lambda
                ((server-index data-index)
                 (let ((pair (list-ref data data-index)))
                   (server-store (list-ref servers server-index)
                                 (car pair)
                                 (cdr pair)))))
            store-order)
           ;; retrieve values
           (for-each
            (match-lambda
                ((server-index data-index)
                 (let ((pair (list-ref data data-index)))
                   (test (cdr pair)
                         (server-find-value (list-ref servers server-index)
                                            (car pair))))))
            find-order)
           ))))))

;; TODO: create network of multiple nodes and try finding specific node making multiple hops
;; TODO: create netowork of multiple nodes and try store/find_value calls across network
;; join mechanism is implicit in the above tests

;; TODO: create procedure to find new unbound port number have
;;       server-start acception port as optional - if defined bind
;;       only to that port (when you've opened that port in your
;;       firewall), otherwise find any unbound random port to use

(test-exit)
