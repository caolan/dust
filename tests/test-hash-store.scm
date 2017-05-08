(use dust.hash-store
     dust.connection
     dust.u8vector-utils
     lmdb-lolevel
     posix
     srfi-4
     srfi-18
     lazy-seq
     sodium
     test
     matchable
     unix-sockets
     test-generative
     data-generators)

;; LMDB does not work RDONLY on OpenBSD and should be used in MDB_WRITEMAP mode
(define openbsd (string=? (car (system-information)) "OpenBSD"))
(define env-flags (if openbsd MDB_WRITEMAP 0))


(define (clear-testdb #!optional (path "tests/testdb"))
  (when (file-exists? path)
    (delete-directory path #t))
  (create-directory path))

(define (with-test-env path thunk)
  (create-directory path #t)
  (let ((env (mdb-env-create)))
    (mdb-env-set-mapsize env 10000000)
    (mdb-env-set-maxdbs env 3)
    (mdb-env-open env path (bitwise-ior env-flags MDB_NOSYNC)
                  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    (handle-exceptions exn
        (begin
          (mdb-env-close env)
          (abort exn))
      (thunk env))
    (mdb-env-close env)))

(define (with-txn env thunk)
  (let ((txn (mdb-txn-begin env #f 0)))
    (handle-exceptions exn
      (begin
        (mdb-txn-abort txn)
        (abort exn))
      (thunk txn))
    (mdb-txn-commit txn)))

(define (with-test-store path thunk)
  (with-test-env
   path
   (lambda (env)
     (with-txn env (compose thunk hash-store-open)))))

(define (copy-store thunk)
  (let ((path1 "tests/testdb")
        (path2 "tests/testdb-copy"))
    (clear-testdb path2)
    (with-test-env path1 (cut mdb-env-copy <> path2))
    (with-test-store
     path2
     (lambda (store2)
       (with-test-store
        path1
        (lambda (store1)
          (thunk store1 store2)))))))

(define (blob-append a b)
  (u8vector->blob/shared
   (u8vector-append (blob->u8vector/shared a)
                    (blob->u8vector/shared b))))

(define (make-test-store tree thunk)
  (clear-testdb)
  (with-test-env
   "tests/testdb"
   (lambda (env)
     (with-txn
      env
      (lambda (txn)
        (let ((dbi (hashes-dbi-open txn)))
          (let loop ((tree tree)
                     (prefix "/"))
            (for-each
             (lambda (node)
               (let* ((subkey (car node))
                      (children (cdr node))
                      (node (make-node 'parent-key
                                       (blob->u8vector (string->blob subkey))
                                       (generic-hash (string->blob "data")
                                                     size: hash-size)
                                       (null? children))))
                 (mdb-put txn dbi (string->blob prefix) (node->blob node) 0)
                 (loop children (string-append prefix subkey))))
             tree))
          (thunk (hash-store-open txn))))))))

(define (dirty-keys store)
  (let* ((dbi (hash-store-dirty-db store))
         (cursor (mdb-cursor-open (hash-store-txn store) dbi)))
    (keys cursor)))

(define (total-dup-count store)
  (let ((count 0))
    (condition-case
        (begin
          (mdb-cursor-get (hash-store-cursor store) #f #f MDB_FIRST)
          (set! count (+ count 1))
          (let loop ()
            (mdb-cursor-get (hash-store-cursor store) #f #f MDB_NEXT)
            (set! count (+ count 1))
            (loop)))
      ((exn lmdb MDB_NOTFOUND) count))))

(define (tree->list store node)
  (map (lambda (child)
         (cons (u8vector->string (node-subkey child))
               (if (node-leaf child)
                   '()
                   (tree->list store child))))
       (lazy-seq->list (node-children store node))))

;; produces a random key 20 bytes long where each byte is in the range
;; 0-2 to encourage clashes and nesting
(define (random-low-variation-key store #!optional size)
  (with-size
   (or size
       (range 20 (hash-store-max-key-size
                  (mdb-txn-env (hash-store-txn store)))))
   (gen-transform (compose u8vector->blob list->u8vector)
                  (gen-list-of (gen-fixnum 0 2)))))

(define (random-hash)
  (with-size
   hash-size
   (gen-transform (compose u8vector->blob list->u8vector)
                  (gen-list-of (gen-uint8)))))


(test-group "node->blob and blob->node"
  (let ((node
         (make-node
          (blob->u8vector (string->blob "parent"))
          (blob->u8vector (string->blob "subkey"))
          (generic-hash (string->blob "data") size: hash-size)
          #t
          ))
        
        (node-empty-subkey
         (make-node
          (blob->u8vector (string->blob "parent"))
          (make-u8vector 0)
          (generic-hash (string->blob "data") size: hash-size)
          #t
          )))
    
    (test-assert (blob? (node->blob node)))
    (test-assert (node? (blob->node (make-node
                                     (string->u8vector "")
                                     (string->u8vector "parent")
                                     (string->blob "hash")
                                     #f)
                                    (node->blob node))))
    (let ((node2 (blob->node (make-node
                              (string->u8vector "")
                              (string->u8vector "parent")
                              (string->blob "hash")
                              #f)
                             (node->blob node))))
      (test (node-leaf node) (node-leaf node2))
      (test (node-subkey node) (node-subkey node2))
      (test (node-stored-hash node) (node-stored-hash node2)))
    (test-assert (blob? (node->blob node-empty-subkey)))
    (test-assert (node? (blob->node (make-node
                                     (string->u8vector "")
                                     (string->u8vector "parent")
                                     (string->blob "hash")
                                     #f)
                                    (node->blob node-empty-subkey))))
    (let ((node2 (blob->node
                  (make-node
                   (string->u8vector "")
                   (string->u8vector "parent")
                   (string->blob "hash")
                                     #f)
                  (node->blob node-empty-subkey))))
      (test (node-leaf node-empty-subkey) (node-leaf node2))
      (test (node-subkey node-empty-subkey) (node-subkey node2))
      (test (node-stored-hash node-empty-subkey) (node-stored-hash node2)))
    (test-error "invalid hash size"
                (node->blob (make-node (string->u8ector "parent")
                                       (make-u8vector 0)
                                       (string->blob "hash")
                                       #t)))
    (test-error "invalid blob size"
                (blob->node (make-node (string->u8vector "")
                                       (string->u8vector "parent")
                                       (string->blob "hash")
                                       #f)
                            (string->blob "node")))))

(test-group "node-key"
  (test (string->u8vector "foobar")
        (node-key
         (make-node
          (string->u8vector "foo")
          (string->u8vector "bar")
          (generic-hash (string->blob "data") size: hash-size)
          #t))))
                                       

(test-group "find-child-with-empty-subkey (present)"
  (make-test-store
   '(("test" . (("" . ())
                ("er" . ()))))
   (lambda (store)
     (let ((test-node (make-node (string->u8vector "")
                                 (string->u8vector "test")
                                 (generic-hash (string->blob "data")
                                               size: hash-size)
                                 #f)))
       (test (make-node (string->u8vector "test")
                        (string->u8vector "")
                        (generic-hash (string->blob "data")
                                      size: hash-size)
                        #t)
             (find-child-with-empty-subkey store test-node))))))

(test-group "find-child-with-empty-subkey (missing)"
  (make-test-store
   '(("test" . (("er" . ())
                ("s" . ()))))
   (lambda (store)
     (let ((test-node (make-node (string->u8vector "")
                                 (string->u8vector "test")
                                 (generic-hash (string->blob "data")
                                               size: hash-size)
                                 #f)))
       (test #f
             (find-child-with-empty-subkey store test-node))))))

(test-group "find-child-by-first-byte (present)"
  (make-test-store
   '(("test" . (("" . ())
                ("er" . ())
                ("s" . ()))))
   (lambda (store)
     (let ((test-node (make-node (string->u8vector "")
                                 (string->u8vector "test")
                                 (generic-hash (string->blob "data")
                                               size: hash-size)
                                 #f)))
       (test (make-node (string->u8vector "test")
                        (string->u8vector "er")
                        (generic-hash (string->blob "data")
                                      size: hash-size)
                        #t)
             (find-child-by-first-byte
              store
              test-node
              (u8vector-ref (string->u8vector "ed") 0)))))))

(test-group "find-child-by-first-byte (missing)"
  (make-test-store
   '(("test" . (("er" . ())
                ("s" . ()))))
   (lambda (store)
     (let ((test-node (make-node (string->u8vector "")
                                 (string->u8vector "test")
                                 (generic-hash (string->blob "data")
                                               size: hash-size)
                                 #f)))
       (test #f
             (find-child-by-first-byte
              store
              test-node
              (u8vector-ref (string->u8vector "ing") 0)))))))

(define (format-search-results results)
  (lazy-seq->list
   (lazy-map (lambda (result)
               (list (car result)
                     (u8vector->string (node-parent-key (cdr result)))
                     (u8vector->string (node-subkey (cdr result)))))
             results)))

(test-group "search empty tree"
  (make-test-store
   '()
   (lambda (store)
     (test '((prefix-match "" ""))
           (format-search-results
            (search store
                    (string->u8vector "foo")
                    #t))))))

(test-group "search tree with exact matching leaf at root"
  (make-test-store
   '(("foo" . ()))
   (lambda (store)
     (test '((prefix-match "" "")
             (exact-match "" "foo"))
           (format-search-results
            (search store
                    (string->u8vector "foo")
                    #t))))))

(test-group "search tree with non-matching leaf at root"
  (make-test-store
   '(("foo" . ()))
   (lambda (store)
     (test '((prefix-match "" ""))
           (format-search-results
            (search store
                    (string->u8vector "bar")
                    #t))))))

(test-group "search tree with partial matching leaf at root"
  (make-test-store
   '(("foo" . ()))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-partial "" "foo"))
           (format-search-results
            (search store
                    (string->u8vector "foobar")
                    #t))))))

(test-group "search tree with nested exact matching leaf"
  (make-test-store
   '(("test" . (("ing" . ())
                ("er" . ())
                ("s" . ()))))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-match "" "test")
             (exact-match "test" "ing"))
           (format-search-results
            (search store
                    (string->u8vector "testing")
                    #t))))))

(test-group "search tree with nested partial matching leaf"
  (make-test-store
   '(("test" . (("er" . ())
                ("s" . ()))))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-match "" "test")
             (prefix-partial "test" "er"))
           (format-search-results
            (search store
                    (string->u8vector "tested")
                    #t))))))

(test-group "search tree with nested exact matching empty leaf node"
  (make-test-store
   '(("test" . (("er" . (("s" . ())
                         ("" . ())))
                ("s" . ()))))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-match "" "test")
             (prefix-match "test" "er")
             (exact-match "tester" ""))
           (format-search-results
            (search store
                    (string->u8vector "tester")
                    #t))))))

(test-group "search tree with nested exact matching non-leaf node"
  (make-test-store
   '(("test" . (("er" . ())
                ("ed" . ()))))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-match "" "test"))
           (format-search-results
            (search store
                    (string->u8vector "test")
                    #t))))))

(test-group "search tree with partial matching non-leaf node"
  (make-test-store
   '(("test" . (("er" . ())
                ("ed" . ()))))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-partial "" "test"))
           (format-search-results
            (search store
                    (string->u8vector "tea")
                    #t))))))

(test-group "search tree with nested node with no partial matching subkey"
  (make-test-store
   '(("test" . (("er" . ()))))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-match "" "test"))
           (format-search-results
            (search store
                    (string->u8vector "tests")
                    #t))))))

(test-group "search tree with root entry not matching"
  (make-test-store
   '(("test" . ()))
   (lambda (store)
     (test '((prefix-match "" ""))
           (format-search-results
            (search store
                    (string->u8vector "foo")
                    #t))))))

(test-group "search tree where parent is leaf"
  (make-test-store
   '(("test" . ()))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-partial "" "test"))
           (format-search-results
            (search store
                    (string->u8vector "tests")
                    #t))))))

(test-group "search tree with nested entry not matching"
  (make-test-store
   '(("test" . (("ing" . ()))))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-match "" "test"))
           (format-search-results
            (search store
                    (string->u8vector "tester")
                    #t))))))

(test-group "search tree with nested entry matching (with find-leaf: #f)"
  (make-test-store
   '(("test" . (("ing" . (("" . ())
                          ("s" . ()))))))
   (lambda (store)
     (test '((prefix-match "" "")
             (prefix-match "" "test")
             (prefix-match "test" "ing"))
           (format-search-results
            (search store
                    (string->u8vector "testing")
                    #f))))))

(test-group "get hash - empty tree"
  (make-test-store
   '()
   (lambda (store)
     (test-error (hash-get store (string->blob "foo"))))))

(test-group "get hash - root leaf node"
  (make-test-store
   '(("test" . ()))
   (lambda (store)
     (let ((hash (generic-hash (string->blob "data") size: hash-size)))
       (test hash (hash-get store (string->blob "test")))))))

(test-group "get hash - root non-leaf node (missing key)"
  (make-test-store
   '(("test" . (("ers" . ())
                ("s" . ()))))
   (lambda (store)
     (test-error (hash-get store (string->blob "test"))))))

(test-group "get hash - nested leaf node"
  (make-test-store
   '(("test" . (("ing" . ())
                ("er" . ()))))
   (lambda (store)
     (let ((hash (generic-hash (string->blob "data") size: hash-size)))
       (test hash (hash-get store (string->blob "testing")))))))

(test-group "get hash - empty leaf node"
  (make-test-store
   '(("test" . (("" . ())
                ("er" . ()))))
   (lambda (store)
     (let ((hash (generic-hash (string->blob "data") size: hash-size)))
       (test hash (hash-get store (string->blob "test")))))))

(define (keys cursor)
  (let loop ((keys '())
             (op MDB_FIRST))
    (if (condition-case
            (begin
              (mdb-cursor-get cursor #f #f op)
              #t)
          ((exn lmdb MDB_NOTFOUND) #f))
        (loop (cons (blob->string (mdb-cursor-key cursor)) keys) MDB_NEXT)
        (reverse keys))))

(test-group "insert - empty tree"
  (make-test-store
   '()
   (lambda (store)
     (hash-put
      store
      (string->blob "foo")
      (generic-hash (string->blob "one") size: hash-size))
     (test '(("foo" . ())) (tree->list store root-node))
     (test 1 (total-dup-count store))
     (test '() (dirty-keys store)))))

(test-group "insert - split single root-level node"
  (make-test-store
   '(("testing" . ()))
   (lambda (store)
     (hash-put
      store
      (string->blob "testers")
      (generic-hash (string->blob "two") size: hash-size))
     (test '(("test" . (("ers" . ())
                        ("ing" . ()))))
           (tree->list store root-node))
     (test 3 (total-dup-count store))
     (test '("test") (dirty-keys store)))))

(test-group "insert - split root node and with existing children"
  (make-test-store
   '(("test" . (("ers" . ())
                ("ing" . ()))))
   (lambda (store)
     (hash-put
      store
      (string->blob "tea")
      (generic-hash (string->blob "data") size: hash-size))
     (test '(("te" . (("a" .())
                      ("st" . (("ers" . ())
                               ("ing" . ()))))))
           (tree->list store root-node))
     (test 5 (total-dup-count store))
     (test '("te") (dirty-keys store)))))

(test-group "insert - split nested node and with existing children"
  (make-test-store
   '(("test" . (("ers" . ())
                ("ing" . ()))))
   (lambda (store)
     (hash-put
      store
      (string->blob "tested")
      (generic-hash (string->blob "data") size: hash-size))
     (test '(("test" . (("e" . (("d" . ())
                                ("rs" . ())))
                        ("ing" . ()))))
           (tree->list store root-node))
     (test 5 (total-dup-count store))
     (test '("teste") (dirty-keys store))
     (hash-put
      store
      (string->blob "tests")
      (generic-hash (string->blob "data") size: hash-size))
     (test '(("test" . (("e" . (("d" . ())
                                ("rs" . ())))
                        ("ing" . ())
                        ("s" . ()))))
           (tree->list store root-node))
     (test 6 (total-dup-count store))
     (test '("teste" "test") (dirty-keys store)))))

(test-group "insert - replace existing node"
  (make-test-store
   '(("test" . (("ers" . ())
                ("ing" . ()))))
   (lambda (store)
     (let ((hash2 (generic-hash (string->blob "data2") size: hash-size)))
       (hash-put
        store
        (string->blob "testers")
        hash2)
       (test '(("test" . (("ers" . ())
                          ("ing" . ()))))
             (tree->list store root-node))
       (test hash2 (hash-get store (string->blob "testers")))
       (test 3 (total-dup-count store))
       (test '("test") (dirty-keys store))))))

(test-group "insert - split resulting in empty subkey"
  (make-test-store
   '(("test" . ()))
   (lambda (store)
     (let ((hash (generic-hash (string->blob "data") size: hash-size))
           (hash2 (generic-hash (string->blob "data2") size: hash-size)))
       (hash-put store (string->blob "tests") hash2)
       (test '(("test" . (("" . ())
                          ("s" . ()))))
             (tree->list store root-node))
       (test hash2 (hash-get store (string->blob "tests")))
       (test hash (hash-get store (string->blob "test")))
       (test 3 (total-dup-count store))
       (test '("test") (dirty-keys store))))))

(test-group "del - delete only root node"
  (make-test-store
   '(("test" . ()))
   (lambda (store)
     (let ((hash2 (generic-hash (string->blob "data2") size: hash-size)))
       (hash-delete store (string->blob "test"))
       (test '() (tree->list store root-node))
       (test 0 (total-dup-count store))
       (test '() (dirty-keys store))))))

(test-group "del - delete leaf and do not collapse node"
  (make-test-store
   '(("test" . (("ers" . ())
                ("ing" . ())
                ("s" . ()))))
   (lambda (store)
     (let ((hash2 (generic-hash (string->blob "data2") size: hash-size)))
       (hash-delete store (string->blob "testers"))
       (test '(("test" . (("ing" . ())
                          ("s" . ()))))
             (tree->list store root-node))
       (test 3 (total-dup-count store))
       (test '("test") (dirty-keys store))))))

(test-group "del - delete leaf and collapse node"
  (make-test-store
   '(("test" . (("ers" . ())
                ("ing" . ()))))
   (lambda (store)
     (let ((hash2 (generic-hash (string->blob "data2") size: hash-size)))
       (hash-delete store (string->blob "testers"))
       (test '(("testing" . ()))
             (tree->list store root-node))
       (test 1 (total-dup-count store))
       (test '() (dirty-keys store))))))

(test-group "del - delete leaf and collapse nodes with children"
  (make-test-store
   '(("test" . (("e" . (("d" . ())
                        ("rs" . ())))
                ("ing" . (("" . ())
                          ("s" . ()))))))
   (lambda (store)
     (let ((hash2 (generic-hash (string->blob "data2") size: hash-size)))
       (hash-delete store (string->blob "testing"))
       (test '(("test" . (("e" . (("d" . ())
                                  ("rs" . ())))
                          ("ings" . ()))))
             (tree->list store root-node))
       (test 5 (total-dup-count store))
       (test '("test") (dirty-keys store))
       (hash-delete store (string->blob "testings"))
       (test '(("teste" . (("d" . ())
                           ("rs" . ()))))
             (tree->list store root-node))
       (test 3 (total-dup-count store))
       (test '() (dirty-keys store))
       (hash-delete store (string->blob "testers"))
       (test '(("tested" . ()))
             (tree->list store root-node))
       (test 1 (total-dup-count store))
       (test '() (dirty-keys store))))))

(test-group "del - do not attempt to collapse at root"
  (make-test-store
   '(("bar" . ())
     ("foo" . ()))
   (lambda (store)
     (let ((hash2 (generic-hash (string->blob "data2") size: hash-size)))
       (hash-delete store (string->blob "foo"))
       (test '(("bar" . ()))
             (tree->list store root-node))
       (test 1 (total-dup-count store))
       (test '() (dirty-keys store))))))

(test-group "del - delete nested key with empty sibiling"
  (make-test-store
   '()
   (lambda (store)
     (hash-put store
               (string->blob "foo")
               (generic-hash (string->blob "foo-data") size: hash-size))
     (hash-put store
               (string->blob "foobar")
               (generic-hash (string->blob "foobar-data") size: hash-size))
     (hash-delete store (string->blob "foobar"))
     (test '(("foo" . ()))
           (tree->list store root-node))
     (test 1 (total-dup-count store))
     (test '() (dirty-keys store)))))

(test-group "inserting then deleting keys results in empty tree"
  (make-test-store
   '()
   (lambda (store)
     (let ((pairs (<- (gen-list-of
                       (gen-pair-of (random-low-variation-key store)
                                    (random-hash))
                       100))))
       (for-each (lambda (pair)
                   (hash-put store (car pair) (cdr pair)))
                 pairs)
       (for-each (lambda (pair)
                   (hash-delete store (car pair)))
                 pairs)
       (test '() (tree->list store root-node))
       (test 0 (total-dup-count store))
       (test '() (dirty-keys store))))))

(test-group "inserted hashes can be retrieved with hash-get"
  (make-test-store
   '()
   (lambda (store)
     (test-generative
         ((pairs (gen-list-of
                  (gen-pair-of (random-low-variation-key store)
                               (random-hash))
                  100)))
       (for-each (lambda (pair)
                   (hash-put store (car pair) (cdr pair)))
                 pairs)
       (for-each (lambda (pair)
                   (test (cdr pair)
                         (hash-get store (car pair))))
                 pairs)))))

(test-group "get root hash with only top level nodes"
  (make-test-store
   '()
   (lambda (store)
     (let ((foo-hash (generic-hash (string->blob "foo") size: hash-size))
           (bar-hash (generic-hash (string->blob "bar") size: hash-size)))
       ;; empty blob hashing doesn't seem to be supported in the
       ;; lmdb version which ships with debian jessie (0.9.14-1)
       ;; (test (generic-hash (string->blob "") size: hash-size)
       ;;       (root-hash store))
       (hash-put store (string->blob "foo") foo-hash)
       (test foo-hash (root-hash store))
       (hash-put store (string->blob "bar") bar-hash)
       (test (generic-hash (blob-append bar-hash foo-hash) size: hash-size)
             (root-hash store))))))

(test-group "rehash tree + prefix-hash"
  (make-test-store
   '()
   (lambda (store)
     (let ((tested-hash (generic-hash (string->blob "tested") size: hash-size))
           (testers-hash (generic-hash (string->blob "testers") size: hash-size))
           (testing-hash (generic-hash (string->blob "testing") size: hash-size))
           (testings-hash (generic-hash (string->blob "testings") size: hash-size)))
       (hash-put store (string->blob "tested") tested-hash)
       (hash-put store (string->blob "testers") testers-hash)
       (hash-put store (string->blob "testing") testing-hash)
       (hash-put store (string->blob "testings") testings-hash)
       (test '(("test" . (("e" . (("d" . ())
                                  ("rs" . ())))
                          ("ing" . (("" . ())
                                    ("s" . ()))))))
             (tree->list store root-node))
       (rehash store)
       (let* ((teste-prefix-hash (generic-hash (blob-append tested-hash testers-hash)
                                               size: hash-size))
              (testing-prefix-hash (generic-hash (blob-append testing-hash testings-hash)
                                                 size: hash-size))
              (test-prefix-hash (generic-hash (blob-append teste-prefix-hash testing-prefix-hash)
                                              size: hash-size)))
         ;; (root-prefix-hash (generic-hash test-prefix-hash size: hash-size)))
         (test teste-prefix-hash
               (prefix-hash store (string->u8vector "teste")))
         (test testing-prefix-hash
               (prefix-hash store (string->u8vector "testing")))
         (test test-prefix-hash
               (prefix-hash store (string->u8vector "test")))
         (test test-prefix-hash
               (root-hash store))
         (test '() (dirty-keys store)))))))

(test-group "inserting a key changes the root hash"
  (make-test-store
   '()
   (lambda (store)
     (let ((pairs (<- (gen-list-of
                       (gen-pair-of (random-low-variation-key store)
                                    (random-hash))
                       100))))
       (for-each (lambda (pair)
                   (hash-put store (car pair) (cdr pair)))
                 pairs)
       (let ((hash1 (root-hash store)))
         ;; insert a random entry
         (hash-put store
                   (<- (random-low-variation-key store))
                   (<- (random-hash)))
         (test-assert (not (equal? hash1 (root-hash store)))))))))

(test-group "deleting a key changes the root hash"
  (make-test-store
   '()
   (lambda (store)
     (let ((pairs (<- (gen-list-of
                       (gen-pair-of (random-low-variation-key store)
                                    (random-hash))
                       100))))
       (for-each (lambda (pair)
                   (hash-put store (car pair) (cdr pair)))
                 pairs)
       (let ((hash1 (root-hash store))
             (pair (list-ref pairs (random (length pairs)))))
         ;; delete a random entry
         (hash-delete store (car pair))
         (test-assert (not (equal? hash1 (root-hash store)))))))))

(test-group "compare hash for differently built trees with same end-state"
  (let ((root-hash1 #f)
        (root-hash2 #f))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store
                 (string->blob "foo")
                 (generic-hash (string->blob "foo-data")
                               size: hash-size))
       (hash-put store
                 (string->blob "bar")
                 (generic-hash (string->blob "bar-data")
                               size: hash-size))
       (hash-put store
                 (string->blob "baz")
                 (generic-hash (string->blob "baz-data")
                               size: hash-size))
       (hash-delete store (string->blob "baz"))
       (hash-put store
                 (string->blob "test")
                 (generic-hash (string->blob "test-data")
                               size: hash-size))
       (hash-put store
                 (string->blob "testing")
                 (generic-hash (string->blob "testing-data")
                               size: hash-size))
       (hash-put store
                 (string->blob "tester")
                 (generic-hash (string->blob "tester-data")
                               size: hash-size))
       (hash-delete store (string->blob "test"))
       (set! root-hash1 (root-hash store))))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store
                 (string->blob "tester")
                 (generic-hash (string->blob "tester-data")
                               size: hash-size))
       (hash-put store
                 (string->blob "bar")
                 (generic-hash (string->blob "bar-data")
                               size: hash-size))
       (hash-put store
                 (string->blob "foo")
                 (generic-hash (string->blob "foo-data")
                               size: hash-size))
       (hash-put store
                 (string->blob "foobar")
                 (generic-hash (string->blob "foobar-data")
                               size: hash-size))
       (hash-put store
                 (string->blob "testing")
                 (generic-hash (string->blob "testing-data")
                               size: hash-size))
       (hash-delete store (string->blob "foobar"))
       (set! root-hash2 (root-hash store))))
    (test root-hash1 (identity root-hash2))))

(test-group "random re-hash calls should not change final result"
  (make-test-store
   '()
   (lambda (store)
     (let ((pairs (<- (gen-list-of
                       (gen-pair-of (random-low-variation-key store)
                                    (random-hash))
                       100))))
       (for-each (lambda (pair)
                   (hash-put store (car pair) (cdr pair)))
                 pairs)
       (rehash store)
       (make-test-store
        '()
        (lambda (store2)
          (let ((hash1 (root-hash store)))
            (for-each (lambda (pair)
                        (hash-put store2 (car pair) (cdr pair))
                        ;; randomly re-hash
                        (when (= 1 (random 2))
                          (rehash store2)))
                      pairs)
            (rehash store2)
            (test hash1 (root-hash store2)))))))))

(test-group "node-children on range bound subtree"
  (make-test-store
   '(("a". ())
     ("b" . (("ar" . ())
             ("ee" . ())))
     ("c" . ())
     ("d" . ()))
   (lambda (store)
     (test "all children"
           '("a" "b" "c" "d")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node))))
     (test "abc->"
           ;; 'a' is a leaf node, so it should be excluded despite being a prefix
           '("b" "c" "d")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     (string->u8vector "abc")
                                     #f))))
     (test "b->"
           ;; 'b' is not a leaf node, so should be included based on matching prefix
           '("b" "c" "d")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     (string->u8vector "bar")
                                     #f))))
     (test "c->"
           '("c" "d")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     (string->u8vector "c")
                                     #f))))
     (test "d->"
           '("d")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     (string->u8vector "d")
                                     #f))))
     (test "e->"
           '()
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     (string->u8vector "e")
                                     #f))))
     (test "->c"
           '("a" "b" "c")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     #f
                                     (string->u8vector "c")))))
     (test "->bb"
           '("a" "b")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     #f
                                     (string->u8vector "bb")))))
     (test "->a"
           '("a")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     #f
                                     (string->u8vector "a")))))
     (test "a->d"
           '("a" "b" "c" "d")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     (string->u8vector "a")
                                     (string->u8vector "d")))))
     (test "abc->def"
           '("b" "c" "d")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     (string->u8vector "abc")
                                     (string->u8vector "def")))))
     (test "b->c"
           '("b" "c")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     (string->u8vector "b")
                                     (string->u8vector "c")))))
     (test "ba->bb"
           '("b")
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store
                                     root-node
                                     (string->u8vector "ba")
                                     (string->u8vector "bb")))))))
  (make-test-store
   '(("aa" . (("1" . ())
              ("2" . ())))
     ("bb" . (("3" . ())
              ("4" . ()))))
   (lambda (store2)
     (test "don't move onto node's sibling using MDB_RANGE (->ff)"
           '()
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store2
                                     ;; node is not root, make sure we don't
                                     ;; accidentally move onto the next node
                                     ;; using MDB RANGE lookups
                                     (make-node (string->u8vector "")
                                                (string->u8vector "ab")
                                                'hash
                                                #f)
                                     #f
                                     (string->u8vector "ff")))))))
  (make-test-store
   '(("foo" . (("1" . ())
               ("2" . ())))
     ("bb" . (("3" . ())
              ("4" . ()))))
   (lambda (store2)
     (test "don't move onto node's sibling using MDB_RANGE (->ff)"
           '()
           (lazy-seq->list
            (lazy-map (lambda (node)
                        (u8vector->string (node-subkey node)))
                      (node-children store2
                                     ;; node is not root, make sure we don't
                                     ;; accidentally move onto the next node
                                     ;; using MDB RANGE lookups
                                     (make-node (string->u8vector "")
                                                (string->u8vector "ab")
                                                'hash
                                                #f)
                                     #f
                                     (string->u8vector "ff"))))))))

(test-group "range bound root-hash example"
  (make-test-store
   '()
   (lambda (store)
     (let* ((tea-hash (generic-hash (string->blob "tea-data")
                                    size: hash-size))
            (test-hash (generic-hash (string->blob "test-data")
                                     size: hash-size))
            (tap-hash (generic-hash (string->blob "tap-data")
                                    size: hash-size))
            (zap-hash (generic-hash (string->blob "zap-data")
                                    size: hash-size))
            (zing-hash (generic-hash (string->blob "zing-data")
                                     size: hash-size))
            (te-hash (generic-hash (blob-append tea-hash test-hash)
                                   size: hash-size))
            ;; (range-root-hash (generic-hash te-hash size: hash-size))
            (hash1 #f)
            (hash2 #f))
       (hash-put store (string->blob "tea") tea-hash)
       (test '(("tea" . ()))
             (tree->list store root-node))
       (set! hash1 (root-hash store))
       (hash-put store (string->blob "test") test-hash)
       (test '(("te" . (("a" . ())
                        ("st" . ()))))
             (tree->list store root-node))
       (set! hash2 (root-hash store))
       ;; (test hash2 (identity range-root-hash))
       (test hash2 (identity te-hash))
       (hash-put store (string->blob "zap") zap-hash)
       (test '(("te" . (("a" . ())
                        ("st" . ())))
               ("zap" . ()))
             (tree->list store root-node))
       (set! hash3 (root-hash store))
       (hash-put store (string->blob "tap") tap-hash)
       (hash-put store (string->blob "zing") zing-hash)
       (test '(("t" . (("ap" . ())
                       ("e" . (("a" . ())
                               ("st" . ())))))
               ("z" . (("ap" . ())
                       ("ing" . ()))))
             (tree->list store root-node))
       (test hash1 (root-hash store
                              (string->u8vector "tea")
                              (string->u8vector "tea")))
       (test hash2 (root-hash store
                              (string->u8vector "tea")
                              (string->u8vector "test")))
       (test hash3 (root-hash store
                              (string->u8vector "tea")
                              (string->u8vector "zap")))
       (test-assert (not (blob=? hash1 (root-hash store))))))))

(test-group "left hash"
  (make-test-store
   '()
   (lambda (store)
     ;; make sure all pairs are unique, otherwise it might
     ;; invalidate a previously recorded root hash for that range
     (let* ((pairs (delete-duplicates
                    (sort (<- (gen-list-of
                               (gen-pair-of (random-low-variation-key store 20)
                                            (random-hash))
                               100))
                          (lambda (a b)
                            (string<? (blob->string (car a))
                                      (blob->string (car b)))))
                    (lambda (a b)
                      (blob=? (car a) (car b)))))
            (root-hashes
             (map (lambda (pair)
                    (hash-put store (car pair) (cdr pair))
                    (root-hash store))
                  pairs)))
       (let loop ((pairs pairs)
                  (hashes root-hashes))
         (unless (null? pairs)
           (test (car hashes)
                 (root-hash store #f (blob->u8vector (caar pairs))))
           (loop (cdr pairs) (cdr hashes))))))))

(test-group "right hash"
  (make-test-store
   '()
   (lambda (store)
     ;; make sure all pairs are unique, otherwise it might
     ;; invalidate a previously recorded root hash for that range
     (let* ((pairs (delete-duplicates
                    (sort (<- (gen-list-of
                               (gen-pair-of (random-low-variation-key store 20)
                                            (random-hash))
                               100))
                          (lambda (a b)
                            ;; sort in reverse order to left-hash test
                            (string<? (blob->string (car b))
                                      (blob->string (car a)))))
                    (lambda (a b)
                      (blob=? (car a) (car b)))))
            (root-hashes
             (map (lambda (pair)
                    (hash-put store (car pair) (cdr pair))
                    (root-hash store))
                  pairs)))
       (let loop ((pairs pairs)
                  (hashes root-hashes))
         (unless (null? pairs)
           (test (car hashes)
                 (root-hash store (blob->u8vector (caar pairs)) #f))
           (loop (cdr pairs) (cdr hashes))))))))

(test-group "ranged hash"
  (test-generative
   ((params (gen-transform
             (lambda (pairs)
               (let* ((pairs (delete-duplicates
                              (sort pairs
                                    (lambda (a b)
                                      (string<? (blob->string (car a))
                                                (blob->string (car b)))))
                              (lambda (a b)
                                (blob=? (car a) (car b)))))
                      (drop-n (<- (gen-fixnum (range 0 (- (length pairs) 1)))))
                      (take-n (<- (gen-fixnum (range 1 (- (length pairs) drop-n))))))
                 (list drop-n take-n pairs)))
             (gen-list-of
              (gen-pair-of (random-low-variation-key #f 20)
                           (random-hash))
              5))))
   ;; make sure all pairs are unique, otherwise it might
   ;; invalidate a previously recorded root hash for that range
   (receive (drop-n take-n pairs) (apply values params)
     (let* ((range-pairs (take (drop pairs drop-n) take-n))
            (hash1 #f))
       ;; build a tree using only the range pairs, and get the root hash
       (make-test-store
        '()
        (lambda (store1)
          (for-each (lambda (pair)
                      (hash-put store1 (car pair) (cdr pair)))
                    range-pairs)
          (set! hash1 (root-hash store1))))
       ;; build a tree with *all* pairs, and get a range hash
       ;; then compare with previous root hash
       (make-test-store
        '()
        (lambda (store2)
             (let ((first-pair (first range-pairs))
                   (last-pair (last range-pairs)))
               (for-each (lambda (pair)
                           (hash-put store2 (car pair) (cdr pair)))
                         pairs)
               (test hash1
                     (root-hash
                      store2
                      (blob->u8vector (car (first range-pairs)))
                      (blob->u8vector (car (last range-pairs))))))))))))

(test-group "range-hash with empty subkeys"
  (make-test-store
   '()
   (lambda (store)
     (let* ((foo-hash (generic-hash (string->blob "foo-data")
                                    size: hash-size))
            (test-hash (generic-hash (string->blob "test-data")
                                     size: hash-size))
            (testing-hash (generic-hash (string->blob "testing-data")
                                        size: hash-size))
            (tests-hash (generic-hash (string->blob "tests-data")
                                      size: hash-size))
            (test-bucket-hash (generic-hash (blob-append
                                             (blob-append test-hash
                                                          testing-hash)
                                             tests-hash)
                                            size: hash-size))
            (hash1 #f))
       (hash-put store (string->blob "test") test-hash)
       (hash-put store (string->blob "testing") testing-hash)
       (hash-put store (string->blob "tests") tests-hash)
       (test '(("test" . (("" . ())
                          ("ing" . ())
                          ("s" . ()))))
             (tree->list store root-node))
       (set! hash1 (root-hash store))
       (hash-put store (string->blob "foo") foo-hash)
       (test hash1 (root-hash store
                              (string->u8vector "test")
                              (string->u8vector "tests")))
       (test (generic-hash (blob-append foo-hash test-hash) size: hash-size)
             (root-hash store
                        (string->u8vector "foo")
                        (string->u8vector "test")))
       ;; (test hash1 root-hash1)
       (test hash1 test-bucket-hash)
       ))))

(test-group "prefix-hash"
  (make-test-store
   '()
   (lambda (store)
     (let ((foo-hash (generic-hash (string->blob "foo-data") size: hash-size))
           (testers-hash (generic-hash (string->blob "testers-data") size: hash-size))
           (tester-hash (generic-hash (string->blob "tester-data") size: hash-size))
           (tested-hash (generic-hash (string->blob "tested-data") size: hash-size))
           (test-hash (generic-hash (string->blob "test-data") size: hash-size)))
       (hash-put store (string->blob "foo") foo-hash)
       (hash-put store (string->blob "testers") testers-hash)
       (hash-put store (string->blob "tester") tester-hash)
       (hash-put store (string->blob "tested") tested-hash)
       (hash-put store (string->blob "test") test-hash)
       (test '(("foo" . ())
               ("test" . (("" . ())
                          ("e" . (("d" . ())
                                  ("r" . (("" . ())
                                          ("s" . ()))))))))
             (tree->list store root-node))
       (test "exact-match"
             tested-hash (prefix-hash store (string->u8vector "tested")))
       (test "prefix-match"
             (generic-hash (blob-append tester-hash testers-hash)
                           size: hash-size)
             (prefix-hash store (string->u8vector "tester")))
       (let* ((tester-bucket-hash
               (generic-hash (blob-append tester-hash testers-hash)
                             size: hash-size))
              (teste-bucket-hash
               (generic-hash (blob-append tested-hash tester-bucket-hash)
                             size: hash-size))
              (test-bucket-hash
               (generic-hash (blob-append test-hash teste-bucket-hash)
                             size: hash-size)))
         (test "prefix-partial"
               test-bucket-hash
               (prefix-hash store (string->u8vector "te"))))
       (test "ranged prefix-hash (prefix is prefix of matched node key)"
             (generic-hash (blob-append test-hash tested-hash)
                           size: hash-size)
             (prefix-hash store
                          (string->u8vector "t")
                          (string->u8vector "abc")
                          (string->u8vector "tested")))
       (test "ranged prefix-hash (prefix equal to matched node key)"
             (generic-hash (blob-append test-hash tested-hash)
                           size: hash-size)
             (prefix-hash store
                          (string->u8vector "test")
                          (string->u8vector "abc")
                          (string->u8vector "tested")))
       (test "ranged prefix-hash at root"
             (generic-hash
              (blob-append foo-hash
                           (generic-hash (blob-append test-hash tested-hash)
                                         size: hash-size))
              size: hash-size)
             (prefix-hash store
                          (string->u8vector "")
                          (string->u8vector "abc")
                          (string->u8vector "tested")))
       
       ))))

(test-group "child-hashes - root with single key should not return child result"
  (make-test-store
   '()
   (lambda (store)
     (let ((foo-hash (generic-hash (string->blob "foo-data") size: hash-size)))
       (hash-put store (string->blob "foo") foo-hash)
       (test `(#(,(string->u8vector "foo") ,foo-hash #t))
             (lazy-seq->list (child-hashes store #u8{})))))))

(test-group "child-hashes"
  (make-test-store
   '()
   (lambda (store)
     (let* ((foo-hash (generic-hash (string->blob "foo-data") size: hash-size))
            (testers-hash (generic-hash (string->blob "testers-data") size: hash-size))
            (tester-hash (generic-hash (string->blob "tester-data") size: hash-size))
            (tested-hash (generic-hash (string->blob "tested-data") size: hash-size))
            (test-hash (generic-hash (string->blob "test-data") size: hash-size))
            (bang-hash (generic-hash (string->blob "bang-data") size: hash-size))
            (bar-hash (generic-hash (string->blob "bar-data") size: hash-size))
            (baz-hash (generic-hash (string->blob "baz-data") size: hash-size))
            (ba-bucket-hash
             (generic-hash (blob-append bang-hash (blob-append bar-hash baz-hash))
                           size: hash-size))
            (tester-bucket-hash
             (generic-hash (blob-append tester-hash testers-hash)
                           size: hash-size))
            (teste-bucket-hash
             (generic-hash (blob-append tested-hash tester-bucket-hash)
                           size: hash-size))
            (test-bucket-hash
             (generic-hash (blob-append test-hash teste-bucket-hash)
                           size: hash-size)))
       (hash-put store (string->blob "foo") foo-hash)
       (hash-put store (string->blob "testers") testers-hash)
       (hash-put store (string->blob "tester") tester-hash)
       (hash-put store (string->blob "tested") tested-hash)
       (hash-put store (string->blob "test") test-hash)
       (hash-put store (string->blob "bang") bang-hash)
       (hash-put store (string->blob "bar") bar-hash)
       (hash-put store (string->blob "baz") baz-hash)
       (test '(("ba" . (("ng" . ())
                        ("r" . ())
                        ("z" . ())))
               ("foo" . ())
               ("test" . (("" . ())
                          ("e" . (("d" . ())
                                  ("r" . (("" . ())
                                          ("s" . ()))))))))
             (tree->list store root-node))
       (test "at root"
             `(#(,(string->u8vector "ba") ,ba-bucket-hash #f)
               #(,(string->u8vector "foo") ,foo-hash #t)
               #(,(string->u8vector "test") ,test-bucket-hash #f))
             (lazy-seq->list
              (child-hashes store (string->u8vector ""))))
       (test "subtree"
             `(#(,(string->u8vector "tested") ,tested-hash #t)
               #(,(string->u8vector "tester") ,tester-bucket-hash #f))
             (lazy-seq->list
              (child-hashes store (string->u8vector "teste"))))
       (test "prefix-partial"
             `(#(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "teste") ,teste-bucket-hash #f))
             (lazy-seq->list
              (child-hashes store (string->u8vector "te"))))
       (test "child with same name as requested prefix (empty subkey)"
             `(#(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "teste") ,teste-bucket-hash #f))
             (lazy-seq->list
              (child-hashes store (string->u8vector "test"))))
       (test "at root (bounded)"
             `(#(,(string->u8vector "foo") ,foo-hash #t)
               #(,(string->u8vector "test") ,test-bucket-hash #f))
             (lazy-seq->list
              (child-hashes store
                            (string->u8vector "")
                            (string->u8vector "cat")
                            (string->u8vector "zap"))))
       (test "at root (bounds are prefix of child subkeys)"
             `(#(,(string->u8vector "ba") ,ba-bucket-hash #f)
               #(,(string->u8vector "foo") ,foo-hash #t)
               #(,(string->u8vector "test") ,test-bucket-hash #f))
             (lazy-seq->list
              (child-hashes store
                            (string->u8vector "")
                            (string->u8vector "bang")
                            (string->u8vector "testers"))))
       (test "non-root (bounded)"
             `(#(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "teste") ,teste-bucket-hash #f))
             (lazy-seq->list
              (child-hashes store
                            (string->u8vector "test")
                            (string->u8vector "cat")
                            (string->u8vector "zap"))))
       (test "at root (bound spliting child prefix)"
             `(#(,(string->u8vector "foo") ,foo-hash #t)
               #(,(string->u8vector "test") ,(generic-hash
                                              (blob-append test-hash tested-hash)
                                              size: hash-size) #f))
             (lazy-seq->list
              (child-hashes store
                            (string->u8vector "")
                            (string->u8vector "cat")
                            (string->u8vector "tested"))))
       (test "if only one child in range, collapse subkey"
             `(#(,(string->u8vector "teste") ,teste-bucket-hash #f))
             (lazy-seq->list
              (child-hashes store
                            (string->u8vector "test")
                            (string->u8vector "tested")
                            (string->u8vector "testers"))))
       (test "if only one nested child in range, collapse multiple subkeys"
             `(#(,(string->u8vector "testers") ,testers-hash #t))
             (lazy-seq->list
              (child-hashes store
                            (string->u8vector "test")
                            (string->u8vector "testerr")
                            (string->u8vector "testers"))))))))

(test-group "leaf-hashes"
  (make-test-store
   '()
   (lambda (store)
     (let* ((foo-hash (generic-hash (string->blob "foo-data") size: hash-size))
            (testers-hash (generic-hash (string->blob "testers-data") size: hash-size))
            (tester-hash (generic-hash (string->blob "tester-data") size: hash-size))
            (tested-hash (generic-hash (string->blob "tested-data") size: hash-size))
            (test-hash (generic-hash (string->blob "test-data") size: hash-size))
            (bang-hash (generic-hash (string->blob "bang-data") size: hash-size))
            (bar-hash (generic-hash (string->blob "bar-data") size: hash-size))
            (baz-hash (generic-hash (string->blob "baz-data") size: hash-size))
            (ba-bucket-hash
             (generic-hash (blob-append bang-hash (blob-append bar-hash baz-hash))
                           size: hash-size))
            (tester-bucket-hash
             (generic-hash (blob-append tester-hash testers-hash)
                           size: hash-size))
            (teste-bucket-hash
             (generic-hash (blob-append tested-hash tester-bucket-hash)
                           size: hash-size))
            (test-bucket-hash
             (generic-hash (blob-append test-hash teste-bucket-hash)
                           size: hash-size)))
       (hash-put store (string->blob "foo") foo-hash)
       (hash-put store (string->blob "testers") testers-hash)
       (hash-put store (string->blob "tester") tester-hash)
       (hash-put store (string->blob "tested") tested-hash)
       (hash-put store (string->blob "test") test-hash)
       (hash-put store (string->blob "bang") bang-hash)
       (hash-put store (string->blob "bar") bar-hash)
       (hash-put store (string->blob "baz") baz-hash)
       (test '(("ba" . (("ng" . ())
                        ("r" . ())
                        ("z" . ())))
               ("foo" . ())
               ("test" . (("" . ())
                          ("e" . (("d" . ())
                                  ("r" . (("" . ())
                                          ("s" . ()))))))))
             (tree->list store root-node))
       (test "at root"
             `(#(,(string->u8vector "bang") ,bang-hash #t)
               #(,(string->u8vector "bar") ,bar-hash #t)
               #(,(string->u8vector "baz") ,baz-hash #t)
               #(,(string->u8vector "foo") ,foo-hash #t)
               #(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "tested") ,tested-hash #t)
               #(,(string->u8vector "tester") ,tester-hash #t)
               #(,(string->u8vector "testers") ,testers-hash #t))
             (lazy-seq->list
              (leaf-hashes store (string->u8vector ""))))
       (test "subtree"
             `(#(,(string->u8vector "tested") ,tested-hash #t)
               #(,(string->u8vector "tester") ,tester-hash #t)
               #(,(string->u8vector "testers") ,testers-hash #t))
             (lazy-seq->list
              (leaf-hashes store (string->u8vector "teste"))))
       (test "prefix-partial"
             `(#(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "tested") ,tested-hash #t)
               #(,(string->u8vector "tester") ,tester-hash #t)
               #(,(string->u8vector "testers") ,testers-hash #t))
             (lazy-seq->list
              (leaf-hashes store (string->u8vector "te"))))
       (test "child with same name as requested prefix (empty subkey)"
             `(#(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "tested") ,tested-hash #t)
               #(,(string->u8vector "tester") ,tester-hash #t)
               #(,(string->u8vector "testers") ,testers-hash #t))
             (lazy-seq->list
              (leaf-hashes store (string->u8vector "test"))))
       (test "at root (bounded)"
             `(#(,(string->u8vector "foo") ,foo-hash #t)
               #(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "tested") ,tested-hash #t)
               #(,(string->u8vector "tester") ,tester-hash #t)
               #(,(string->u8vector "testers") ,testers-hash #t))
             (lazy-seq->list
              (leaf-hashes store
                           (string->u8vector "")
                           (string->u8vector "cat")
                           (string->u8vector "zap"))))
       (test "at root (bounds are prefix of child subkeys)"
             `(#(,(string->u8vector "bang") ,bang-hash #t)
               #(,(string->u8vector "bar") ,bar-hash #t)
               #(,(string->u8vector "baz") ,baz-hash #t)
               #(,(string->u8vector "foo") ,foo-hash #t)
               #(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "tested") ,tested-hash #t))
             (lazy-seq->list
              (leaf-hashes store
                           (string->u8vector "")
                           (string->u8vector "bang")
                           (string->u8vector "tested"))))
       (test "non-root (bounded)"
             `(#(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "tested") ,tested-hash #t)
               #(,(string->u8vector "tester") ,tester-hash #t)
               #(,(string->u8vector "testers") ,testers-hash #t))
             (lazy-seq->list
              (leaf-hashes store
                           (string->u8vector "test")
                           (string->u8vector "cat")
                           (string->u8vector "zap"))))
       (test "at root (bound spliting child prefix)"
             `(#(,(string->u8vector "foo") ,foo-hash #t)
               #(,(string->u8vector "test") ,test-hash #t)
               #(,(string->u8vector "tested") ,tested-hash #t))
             (lazy-seq->list
              (leaf-hashes store
                           (string->u8vector "")
                           (string->u8vector "cat")
                           (string->u8vector "tested"))))))))

(test-group "diff - L is null, R is leaf"
  ;; write an empty store
  (make-test-store '() (lambda (store) #t))
  (copy-store
   (lambda (store1 store2)
     (let ((foo-hash (generic-hash (string->blob "one") size: hash-size)))
       (hash-put store2 (string->blob "foo") foo-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(new ,(string->blob "foo") ,foo-hash))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - R is null, L is leaf"
  (make-test-store '() (lambda (store) #t))
  (copy-store
   (lambda (store1 store2)
     (let ((one-hash (generic-hash (string->blob "one") size: hash-size)))
       (hash-put store1 (string->blob "foo") one-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(missing ,(string->blob "foo") #f))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L < R"
  (make-test-store '() (lambda (store) #t))
  (copy-store
   (lambda (store1 store2)
     (let ((one-hash (generic-hash (string->blob "one") size: hash-size))
           (two-hash (generic-hash (string->blob "two") size: hash-size)))
       (hash-put store1 (string->blob "aaa") one-hash)
       (hash-put store2 (string->blob "bbb") two-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(missing ,(string->blob "aaa") #f)
                              #(new ,(string->blob "bbb") ,two-hash))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L > R"
  (make-test-store '() (lambda (store) #t))
  (copy-store
   (lambda (store1 store2)
     (let ((one-hash (generic-hash (string->blob "one") size: hash-size))
           (two-hash (generic-hash (string->blob "two") size: hash-size)))
       (hash-put store2 (string->blob "aaa") one-hash)
       (hash-put store1 (string->blob "bbb") two-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(new ,(string->blob "aaa") ,one-hash)
                              #(missing ,(string->blob "bbb") #f))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L == R, L is leaf, R is leaf, same hash"
  (make-test-store
   '()
   (lambda (store)
     (let ((data-hash (generic-hash (string->blob "data") size: hash-size)))
       (hash-put store (string->blob "asdf") data-hash))))
  (copy-store
   (lambda (store1 store2)
     (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
       (let ((store1-thread
              (make-thread
               (lambda ()
                 (with-connection
                  s1-in
                  s1-out
                  (lambda (conn)
                    (test `()
                          (lazy-seq->list (hash-diff store1 conn))))))
               'diff))
             (store2-thread
              (make-thread
               (lambda ()
                 (with-connection
                  s2-in
                  s2-out
                  (lambda (conn)
                    (hash-diff-accept store2 conn))))
               'accept)))
         (thread-start! store1-thread)
         (thread-start! store2-thread)
         (thread-join! store1-thread)
         (thread-join! store2-thread))))))

(test-group "diff - L == R, L is leaf, R is leaf, different hash"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (data2-hash (generic-hash (string->blob "data2") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "asdf") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store2 (string->blob "asdf") data2-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(different ,(string->blob "asdf") ,data2-hash))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L is null, R is not leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "asdf") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store2 (string->blob "foo") one-hash)
       (hash-put store2 (string->blob "foobar") two-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(new ,(string->blob "foo") ,one-hash)
                              #(new ,(string->blob "foobar") ,two-hash))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - R is null, L is not leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "asdf") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store1 (string->blob "foo") one-hash)
       (hash-put store1 (string->blob "foobar") two-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(missing ,(string->blob "foo") #f)
                              #(missing ,(string->blob "foobar") #f))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L < R, L is not leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store1 (string->blob "foo") one-hash)
       (hash-put store1 (string->blob "foobar") two-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(missing ,(string->blob "foo") #f)
                              #(missing ,(string->blob "foobar") #f))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L > R, R is not leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
           (one-hash (generic-hash (string->blob "one") size: hash-size))
           (two-hash (generic-hash (string->blob "two") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store2 (string->blob "foo") one-hash)
       (hash-put store2 (string->blob "foobar") two-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(new ,(string->blob "foo") ,one-hash)
                              #(new ,(string->blob "foobar") ,two-hash))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L == R, L is not leaf, R is not leaf, same hash"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)
       (hash-put store (string->blob "foo") one-hash)
       (hash-put store (string->blob "foobar") two-hash)))
    (copy-store
     (lambda (store1 store2)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `()
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L == R, L is not leaf, R is not leaf, different hash"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size))
        (three-hash (generic-hash (string->blob "three") size: hash-size))
        (four-hash (generic-hash (string->blob "four") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)
       (hash-put store (string->blob "foo") one-hash)
       (hash-put store (string->blob "foobar") two-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store1 (string->blob "fool") three-hash)
       (hash-put store2 (string->blob "food") four-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(new ,(string->blob "food") ,four-hash)
                              #(missing ,(string->blob "fool") #f))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L is prefix of R, L is leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (let ((one-hash (generic-hash (string->blob "one") size: hash-size)))
         (hash-put store2 (string->blob "foobar") one-hash)
         (hash-put store1 (string->blob "foo") two-hash)
         (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
           (let ((store1-thread
                  (make-thread
                   (lambda ()
                     (with-connection
                      s1-in
                      s1-out
                      (lambda (conn)
                        (test `(#(missing ,(string->blob "foo") #f)
                                #(new ,(string->blob "foobar") ,one-hash))
                              (lazy-seq->list (hash-diff store1 conn))))))
                   'diff))
                 (store2-thread
                  (make-thread
                   (lambda ()
                     (with-connection
                      s2-in
                      s2-out
                      (lambda (conn)
                        (hash-diff-accept store2 conn))))
                   'accept)))
             (thread-start! store1-thread)
             (thread-start! store2-thread)
             (thread-join! store1-thread)
             (thread-join! store2-thread))))))))

(test-group "diff - L is prefix of R, L is not leaf, R is leaf, L does not contain R"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size))
        (three-hash (generic-hash (string->blob "three") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (let ((three-hash (generic-hash (string->blob "three") size: hash-size)))
         (hash-put store1 (string->blob "foobar") one-hash)
         (hash-put store1 (string->blob "foobaz") two-hash)
         (hash-put store2 (string->blob "foobab") three-hash)
         (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
           (let ((store1-thread
                  (make-thread
                   (lambda ()
                     (with-connection
                      s1-in
                      s1-out
                      (lambda (conn)
                        (test `(#(new ,(string->blob "foobab") ,three-hash)
                                #(missing ,(string->blob "foobar") #f)
                                #(missing ,(string->blob "foobaz") #f))
                              (lazy-seq->list (hash-diff store1 conn))))))
                   'diff))
                 (store2-thread
                  (make-thread
                   (lambda ()
                     (with-connection
                      s2-in
                      s2-out
                      (lambda (conn)
                        (hash-diff-accept store2 conn))))
                   'accept)))
             (thread-start! store1-thread)
             (thread-start! store2-thread)
             (thread-join! store1-thread)
             (thread-join! store2-thread))))))))

(test-group "diff - L is prefix of R, L is not leaf, R is leaf, L contains R"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store1 (string->blob "foobar") one-hash)
       (hash-put store1 (string->blob "foobaz") two-hash)
       (hash-put store2 (string->blob "foobar") one-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(missing ,(string->blob "foobaz") #f))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L is prefix of R, L is not leaf, R is not a leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size))
        (three-hash (generic-hash (string->blob "three") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store1 (string->blob "foobar") one-hash)
       (hash-put store1 (string->blob "food") two-hash)
       (hash-put store2 (string->blob "foobar") one-hash)
       (hash-put store2 (string->blob "foobaz") three-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(missing ,(string->blob "food") #f)
                              #(new ,(string->blob "foobaz") ,three-hash))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - R is prefix of L, R is leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store2 (string->blob "foo") two-hash)
       (hash-put store1 (string->blob "foobar") one-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(new ,(string->blob "foo") ,two-hash)
                              #(missing ,(string->blob "foobar") #f))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - R is prefix of L, R is not leaf, L is leaf, R does not contain L"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size))
        (three-hash (generic-hash (string->blob "three") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store2 (string->blob "foobar") one-hash)
       (hash-put store2 (string->blob "foobaz") two-hash)
       (hash-put store1 (string->blob "foobab") three-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(missing ,(string->blob "foobab") #f)
                              #(new ,(string->blob "foobar") ,one-hash)
                              #(new ,(string->blob "foobaz") ,two-hash))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - R is prefix of L, R is not leaf, L is leaf, R contains L"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store2 (string->blob "foobar") one-hash)
       (hash-put store2 (string->blob "foobaz") two-hash)
       (hash-put store1 (string->blob "foobar") one-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(new ,(string->blob "foobaz") ,two-hash))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - R is prefix of L, R is not leaf, L is not a leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size))
        (three-hash (generic-hash (string->blob "three") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (let ((two-hash (generic-hash (string->blob "two") size: hash-size)))
         (hash-put store2 (string->blob "foobar") one-hash)
         (hash-put store2 (string->blob "food") two-hash)
         (hash-put store1 (string->blob "foobar") one-hash)
         (hash-put store1 (string->blob "foobaz") three-hash)
         (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
           (let ((store1-thread
                  (make-thread
                   (lambda ()
                     (with-connection
                      s1-in
                      s1-out
                      (lambda (conn)
                        (test `(#(new ,(string->blob "food") ,two-hash)
                                #(missing ,(string->blob "foobaz") #f))
                              (lazy-seq->list (hash-diff store1 conn))))))
                   'diff))
                 (store2-thread
                  (make-thread
                   (lambda ()
                     (with-connection
                      s2-in
                      s2-out
                      (lambda (conn)
                        (hash-diff-accept store2 conn))))
                   'accept)))
             (thread-start! store1-thread)
             (thread-start! store2-thread)
             (thread-join! store1-thread)
             (thread-join! store2-thread))))))))

(test-group "diff - L == R, L is leaf, R is not leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size))
        (three-hash (generic-hash (string->blob "three") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store1 (string->blob "foo") one-hash)
       (hash-put store2 (string->blob "foobar") two-hash)
       (hash-put store2 (string->blob "food") three-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(missing ,(string->blob "foo") #f)
                              #(new ,(string->blob "foobar") ,two-hash)
                              #(new ,(string->blob "food") ,three-hash))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "diff - L == R, L is leaf, R is not leaf"
  (let ((data-hash (generic-hash (string->blob "data") size: hash-size))
        (one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size))
        (three-hash (generic-hash (string->blob "three") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "test") data-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store1 (string->blob "foobar") two-hash)
       (hash-put store1 (string->blob "food") three-hash)
       (hash-put store2 (string->blob "foo") one-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(new ,(string->blob "foo") ,one-hash)
                              #(missing ,(string->blob "foobar") #f)
                              #(missing ,(string->blob "food") #f))
                            (lazy-seq->list (hash-diff store1 conn))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "bounded diff"
  (let ((one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size))
        (three-hash (generic-hash (string->blob "three") size: hash-size))
        (four-hash (generic-hash (string->blob "four") size: hash-size))
        (five-hash (generic-hash (string->blob "five") size: hash-size))
        (six-hash (generic-hash (string->blob "six") size: hash-size))
        (seven-hash (generic-hash (string->blob "seven") size: hash-size))
        (eight-hash (generic-hash (string->blob "eight") size: hash-size))
        (nine-hash (generic-hash (string->blob "nine") size: hash-size))
        (ten-hash (generic-hash (string->blob "ten") size: hash-size))
        (eleven-hash (generic-hash (string->blob "eleven") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "abc") one-hash)
       (hash-put store (string->blob "test") two-hash)
       (hash-put store (string->blob "tested") three-hash)
       (hash-put store (string->blob "tester") four-hash)
       (hash-put store (string->blob "testing") five-hash)
       (hash-put store (string->blob "tests") six-hash)
       (hash-put store (string->blob "zap") seven-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-put store2 (string->blob "foo") nine-hash)
       (hash-put store2 (string->blob "test") ten-hash)
       (hash-delete store2 (string->blob "tester"))
       (hash-put store2 (string->blob "testers") eight-hash)
       (hash-put store2 (string->blob "testing") eleven-hash)
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(different ,(string->blob "testing") ,eleven-hash)
                              #(missing ,(string->blob "tester") #f)
                              #(new ,(string->blob "testers") ,eight-hash))
                            (lazy-seq->list
                             (hash-diff store1
                                        conn
                                        (string->u8vector "tested")
                                        (string->u8vector "tests")))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))

(test-group "bounded diff 2"
  (let ((one-hash (generic-hash (string->blob "one") size: hash-size))
        (two-hash (generic-hash (string->blob "two") size: hash-size))
        (three-hash (generic-hash (string->blob "three") size: hash-size))
        (four-hash (generic-hash (string->blob "four") size: hash-size))
        (five-hash (generic-hash (string->blob "five") size: hash-size)))
    (make-test-store
     '()
     (lambda (store)
       (hash-put store (string->blob "teaparty") one-hash)
       (hash-put store (string->blob "teapot") two-hash)
       (hash-put store (string->blob "teaset") three-hash)
       (hash-put store (string->blob "tester") four-hash)
       (hash-put store (string->blob "testing") five-hash)))
    (copy-store
     (lambda (store1 store2)
       (hash-delete store2 (string->blob "teaset"))
       (hash-delete store2 (string->blob "tester"))
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
         (let ((store1-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s1-in
                    s1-out
                    (lambda (conn)
                      (test `(#(missing "teaset" #f))
                            (map (lambda (x)
                                   (vector-set! x 1 (blob->string (vector-ref x 1)))
                                   x)
                                 (lazy-seq->list
                                  (hash-diff store1
                                             conn
                                             #f
                                             (string->u8vector "teaset"))))))))
                 'diff))
               (store2-thread
                (make-thread
                 (lambda ()
                   (with-connection
                    s2-in
                    s2-out
                    (lambda (conn)
                      (hash-diff-accept store2 conn))))
                 'accept)))
           (thread-start! store1-thread)
           (thread-start! store2-thread)
           (thread-join! store1-thread)
           (thread-join! store2-thread)))))))


(define (select-pair-not-in-operations pairs ops)
  (let ((pair (list-ref pairs (random (length pairs)))))
    (if (member pair
                ops
                (lambda (pair x)
                  (blob=? (first pair) (second x))))
        ;; try again
        (select-pair-not-in-operations pairs ops)
        pair)))

(define ((gen-random-operations pairs n))
  (let loop ((ops '())
             (n n))
    (if (= n 0)
        ops
        (case (random 3)
          ((0) ;; create
           (loop (cons `(create ,(<- (random-low-variation-key #f 20))
                                ,(make-blob 20))
                       ops)
                 (- n 1)))
          ((1) ;; update
           (let ((pair (select-pair-not-in-operations pairs ops)))
             (loop (cons `(update ,(car pair)
                                  ,(make-blob 20))
                         ops)
                   (- n 1))))
          ((2) ;; delete
           (let ((pair (select-pair-not-in-operations pairs ops)))
             (loop (cons `(delete ,(car pair))
                         ops)
                   (- n 1))))))))

(test-group "detect random create/update/delete operations using diff"
  ;; initialize a database with random data
  (let ((pairs (<- (gen-transform
                    (lambda (pairs)
                      (delete-duplicates
                       (sort pairs
                             (lambda (a b)
                               (string<? (blob->string (car a))
                                         (blob->string (car b)))))
                       (lambda (a b)
                         (blob=? (car a) (car b)))))
                    (gen-list-of
                     (gen-pair-of (random-low-variation-key #f 20)
                                  (random-hash))
                     100)))))
    (make-test-store
     '()
     (lambda (store)
       (for-each (lambda (pair)
                   (hash-put store (car pair) (cdr pair)))
                 pairs)))
    ;; test some random operations
    (test-generative
        ((operations (gen-random-operations pairs 20)))
      (copy-store
       (lambda (store1 store2)
         (for-each
          (match-lambda
              (('create key hash)
               (hash-put store2 key hash))
            (('update key hash)
             (hash-put store2 key hash))
            (('delete key)
             (hash-delete store2 key)))
          operations)
         (let ((expected
                (sort
                 (map (match-lambda
                          (('create key hash)
                           (vector 'new key hash))
                        (('update key hash)
                         (vector 'different key hash))
                        (('delete key)
                         (vector 'missing key #f)))
                      operations)
                 (lambda (a b)
                   (string<? (blob->string (vector-ref a 1))
                             (blob->string (vector-ref b 1)))))))
           (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
             (let ((store1-thread
                    (make-thread
                     (lambda ()
                       (with-connection
                        s1-in
                        s1-out
                        (lambda (conn)
                          (test expected
                                (sort
                                 (lazy-seq->list (hash-diff store1 conn))
                                 (lambda (a b)
                                   (string<? (blob->string (vector-ref a 1))
                                             (blob->string (vector-ref b 1)))))))))
                     'diff))
                   (store2-thread
                    (make-thread
                     (lambda ()
                       (with-connection
                        s2-in
                        s2-out
                        (lambda (conn)
                          (hash-diff-accept store2 conn))))
                     'accept)))
               (thread-start! store1-thread)
               (thread-start! store2-thread)
               (thread-join! store1-thread)
               (thread-join! store2-thread)))))))))

(test-group "random create/update/delete operations and bounded diff"
  ;; test some random operations
  (test-generative
      ((params (gen-transform
                (match-lambda
                    ((pairs operations)
                     (let* ((start-index
                             (and (not (= 0 (random 2)))
                                  (random (length operations))))
                            (end-index
                             (and (not (= 0 (random 2)))
                                  (if start-index
                                      (+ start-index
                                         (random (- (length operations)
                                                    start-index)))
                                      (random (length operations)))))
                            (start (and start-index
                                        (second (list-ref operations start-index))))
                            (end (and end-index
                                      (second (list-ref operations end-index)))))
                       (list pairs operations start end))))
                (gen-transform
                 (lambda (pairs)
                   (list pairs
                         (sort (<- (gen-random-operations pairs 25))
                               (lambda (a b)
                                 (string<? (blob->string (second a))
                                           (blob->string (second b)))))))
                 (gen-transform
                  (lambda (pairs)
                    (delete-duplicates
                     (sort pairs
                           (lambda (a b)
                             (string<? (blob->string (car a))
                                       (blob->string (car b)))))
                     (lambda (a b)
                       (blob=? (car a) (car b)))))
                  (gen-list-of
                   (gen-pair-of (random-low-variation-key #f 20)
                                (random-hash))
                   100))))))
    ;; initialize a database with random data
    (receive (pairs operations start end) (apply values params)
      (make-test-store
       '()
       (lambda (store)
         (for-each (lambda (pair)
                     (hash-put store (car pair) (cdr pair)))
                   pairs)))
      (copy-store
       (lambda (store1 store2)
         (for-each
          (match-lambda
            (('create key hash)
             (hash-put store2 key hash))
            (('update key hash)
             (hash-put store2 key hash))
            (('delete key)
             (hash-delete store2 key)))
             operations)
         (let ((expected
                (filter
                 (lambda (x)
                   (and
                    (or (not start)
                        (string>=? (blob->string (vector-ref x 1))
                                   (blob->string start)))
                    (or (not end)
                        (string<=? (blob->string (vector-ref x 1))
                                   (blob->string end)))))
                 (map (match-lambda
                          (('create key hash)
                           (vector 'new key hash))
                        (('update key hash)
                         (vector 'different key hash))
                        (('delete key)
                         (vector 'missing key #f)))
                      operations))))
           (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
             (let ((store1-thread
                    (make-thread
                     (lambda ()
                       (with-connection
                        s1-in
                        s1-out
                        (lambda (conn)
                          (test expected
                                (sort
                                 (lazy-seq->list
                                  (hash-diff store1
                                             conn
                                             (and start (blob->u8vector start))
                                             (and end (blob->u8vector end))))
                                 (lambda (a b)
                                   (string<? (blob->string (vector-ref a 1))
                                             (blob->string (vector-ref b 1)))))))))
                     'diff))
                   (store2-thread
                    (make-thread
                     (lambda ()
                       (with-connection
                        s2-in
                        s2-out
                        (lambda (conn)
                          (hash-diff-accept store2 conn))))
                     'accept)))
               (thread-start! store1-thread)
               (thread-start! store2-thread)
               (thread-join! store1-thread)
               (thread-join! store2-thread)))))))))

(test-exit)
