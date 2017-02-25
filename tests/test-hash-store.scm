(use dust.hash-store
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


(define (clear-testdb #!optional (path "tests/testdb"))
  (when (file-exists? path)
    (delete-directory path #t))
  (create-directory path))

(define (open-test-env path)
  (let ((env (mdb-env-create)))
    (mdb-env-set-mapsize env 10000000)
    (mdb-env-set-maxdbs env 3)
    (mdb-env-open env path 0
		  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    env))

(define (open-test-store path)
  (let* ((env (open-test-env path))
	 (txn (mdb-txn-begin env #f 0)))
    (hash-store-open txn)))

(define (reopen-store store)
  (let* ((env (mdb-txn-env (hash-store-txn store)))
	 (new-txn (mdb-txn-begin env #f 0)))
    (hash-store-open new-txn)))

(define (make-copy-store store)
  (let ((path "tests/testdb-copy"))
    (clear-testdb path)
    (mdb-env-copy (mdb-txn-env (hash-store-txn store)) path)
    (open-test-store path)))

(define (store-copy-path from to)
  (let* ((env (open-test-env from)))
    (mdb-env-copy env to)
    (mdb-env-close env)))

(define (blob-append a b)
  (u8vector->blob/shared
   (u8vector-append (blob->u8vector/shared a)
		    (blob->u8vector/shared b))))

(define (make-test-store #!optional (tree '()))
  (clear-testdb)
  (let ((env (open-test-env "tests/testdb")))
    (let* ((txn (mdb-txn-begin env #f 0))
	   (dbi (hashes-dbi-open txn)))
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
	 tree)
	(hash-store-open txn)))))

(define (dirty-keys store)
  (let* ((dbi (hash-store-dirty-db store))
	 (cursor (mdb-cursor-open (hash-store-txn store) dbi)))
    (keys cursor)))

(define (dup-count store)
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
  (let* ((store (make-test-store
		 '(("test" . (("" . ())
			      ("er" . ()))))))
	 (test-node (make-node (string->u8vector "")
			       (string->u8vector "test")
			       (generic-hash (string->blob "data")
						 size: hash-size)
			       #f)))
    (test (make-node (string->u8vector "test")
		     (string->u8vector "")
		     (generic-hash (string->blob "data")
						 size: hash-size)
		     #t)
	  (find-child-with-empty-subkey store test-node))))

(test-group "find-child-with-empty-subkey (missing)"
  (let* ((store (make-test-store
		 '(("test" . (("er" . ())
			      ("s" . ()))))))
	 (test-node (make-node (string->u8vector "")
			       (string->u8vector "test")
			       (generic-hash (string->blob "data")
						 size: hash-size)
			       #f)))
    (test #f
	  (find-child-with-empty-subkey store test-node))))

(test-group "find-child-by-first-byte (present)"
  (let* ((store (make-test-store
		 '(("test" . (("" . ())
			      ("er" . ())
			      ("s" . ()))))))
	 (test-node (make-node (string->u8vector "")
			       (string->u8vector "test")
			       (generic-hash (string->blob "data")
						 size: hash-size)
			       #f)))
    (test (make-node (string->u8vector "test")
		     (string->u8vector "er")
		     (generic-hash (string->blob "data")
						 size: hash-size)
		     #t)
	  (find-child-by-first-byte store
				    test-node
				    (u8vector-ref (string->u8vector "ed") 0)))))

(test-group "find-child-by-first-byte (missing)"
  (let* ((store (make-test-store
		 '(("test" . (("er" . ())
			      ("s" . ()))))))
	 (test-node (make-node (string->u8vector "")
			       (string->u8vector "test")
			       (generic-hash (string->blob "data")
						 size: hash-size)
			       #f)))
    (test #f
	  (find-child-by-first-byte store
				    test-node
				    (u8vector-ref (string->u8vector "ing") 0)))))

(define (format-search-results results)
  (lazy-seq->list
   (lazy-map (lambda (result)
	       (list (car result)
		     (u8vector->string (node-parent-key (cdr result)))
		     (u8vector->string (node-subkey (cdr result)))))
	     results)))

(test-group "search empty tree"
  (let ((store (make-test-store '())))
    (test '((prefix-match "" ""))
	  (format-search-results
	   (search store
		   (string->u8vector "foo")
		   #t)))))

(test-group "search tree with exact matching leaf at root"
  (let ((store (make-test-store '(("foo" . ())))))
    (test '((prefix-match "" "")
	    (exact-match "" "foo"))
	  (format-search-results
	   (search store
		   (string->u8vector "foo")
		   #t)))))

(test-group "search tree with non-matching leaf at root"
  (let ((store (make-test-store '(("foo" . ())))))
    (test '((prefix-match "" ""))
	  (format-search-results
	   (search store
		   (string->u8vector "bar")
		   #t)))))

(test-group "search tree with partial matching leaf at root"
  (let ((store (make-test-store '(("foo" . ())))))
    (test '((prefix-match "" "")
	    (prefix-partial "" "foo"))
	  (format-search-results
	   (search store
		   (string->u8vector "foobar")
		   #t)))))

(test-group "search tree with nested exact matching leaf"
  (let ((store (make-test-store
		'(("test" . (("ing" . ())
			     ("er" . ())
			     ("s" . ())))))))
    (test '((prefix-match "" "")
	    (prefix-match "" "test")
	    (exact-match "test" "ing"))
	  (format-search-results
	   (search store
		   (string->u8vector "testing")
		   #t)))))

(test-group "search tree with nested partial matching leaf"
  (let ((store (make-test-store
		'(("test" . (("er" . ())
			     ("s" . ())))))))
    (test '((prefix-match "" "")
	    (prefix-match "" "test")
	    (prefix-partial "test" "er"))
	  (format-search-results
	   (search store
		   (string->u8vector "tested")
		   #t)))))

(test-group "search tree with nested exact matching empty leaf node"
  (let ((store (make-test-store
		'(("test" . (("er" . (("s" . ())
				      ("" . ())))
			     ("s" . ())))))))
    (test '((prefix-match "" "")
	    (prefix-match "" "test")
	    (prefix-match "test" "er")
	    (exact-match "tester" ""))
	  (format-search-results
	   (search store
		   (string->u8vector "tester")
		   #t)))))

(test-group "search tree with nested exact matching non-leaf node"
  (let ((store (make-test-store
		'(("test" . (("er" . ())
			     ("ed" . ())))))))
    (test '((prefix-match "" "")
	    (prefix-match "" "test"))
	  (format-search-results
	   (search store
		   (string->u8vector "test")
		   #t)))))

(test-group "search tree with partial matching non-leaf node"
  (let ((store (make-test-store
		'(("test" . (("er" . ())
			     ("ed" . ())))))))
    (test '((prefix-match "" "")
	    (prefix-partial "" "test"))
	  (format-search-results
	   (search store
		   (string->u8vector "tea")
		   #t)))))

(test-group "search tree with nested node with no partial matching subkey"
  (let ((store (make-test-store '(("test" . (("er" . ())))))))
    (test '((prefix-match "" "")
	    (prefix-match "" "test"))
	  (format-search-results
	   (search store
		   (string->u8vector "tests")
		   #t)))))

(test-group "search tree with root entry not matching"
  (let ((store (make-test-store '(("test" . ())))))
    (test '((prefix-match "" ""))
	  (format-search-results
	   (search store
		   (string->u8vector "foo")
		   #t)))))

(test-group "search tree where parent is leaf"
  (let ((store (make-test-store '(("test" . ())))))
    (test '((prefix-match "" "")
	    (prefix-partial "" "test"))
	  (format-search-results
	   (search store
		   (string->u8vector "tests")
		   #t)))))

(test-group "search tree with nested entry not matching"
  (let ((store (make-test-store '(("test" . (("ing" . ())))))))
    (test '((prefix-match "" "")
	    (prefix-match "" "test"))
	  (format-search-results
	   (search store
		   (string->u8vector "tester")
		   #t)))))

(test-group "search tree with nested entry matching (with find-leaf: #f)"
  (let ((store (make-test-store '(("test" . (("ing" . (("" . ())
						       ("s" . ())))))))))
    (test '((prefix-match "" "")
	    (prefix-match "" "test")
	    (prefix-match "test" "ing"))
	  (format-search-results
	   (search store
		   (string->u8vector "testing")
		   #f)))))

(test-group "get hash - empty tree"
  (let ((store (make-test-store '())))
    (test-error (hash-get store (string->blob "foo")))))

(test-group "get hash - root leaf node"
  (let ((store (make-test-store '(("test" . ()))))
    	(hash (generic-hash (string->blob "data") size: hash-size)))
    (test hash (hash-get store (string->blob "test")))))

(test-group "get hash - root non-leaf node (missing key)"
  (let ((store (make-test-store '(("test" . (("ers" . ())
					     ("s" . ())))))))
    (test-error (hash-get store (string->blob "test")))))

(test-group "get hash - nested leaf node"
  (let ((store (make-test-store '(("test" . (("ing" . ())
					     ("er" . ()))))))
    	(hash (generic-hash (string->blob "data") size: hash-size)))
    (test hash (hash-get store (string->blob "testing")))))

(test-group "get hash - empty leaf node"
  (let ((store (make-test-store '(("test" . (("" . ())
					     ("er" . ()))))))
    	(hash (generic-hash (string->blob "data") size: hash-size)))
    (test hash (hash-get store (string->blob "test")))))

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
  (let* ((store (make-test-store '())))
    (hash-put
     store
     (string->blob "foo")
     (generic-hash (string->blob "one") size: hash-size))
    (test '(("foo" . ())) (tree->list store root-node))
    (test 1 (dup-count store))
    (test '() (dirty-keys store))))

(test-group "insert - split single root-level node"
  (let ((store (make-test-store '(("testing" . ())))))
    (hash-put
     store
     (string->blob "testers")
     (generic-hash (string->blob "two") size: hash-size))
    (test '(("test" . (("ers" . ())
		       ("ing" . ()))))
	  (tree->list store root-node))
    (test 3 (dup-count store))
    (test '("test") (dirty-keys store))
    (mdb-txn-commit (hash-store-txn store))))

(test-group "insert - split root node and with existing children"
  (let ((store (make-test-store
		'(("test" . (("ers" . ())
			     ("ing" . ())))))))
    (hash-put
     store
     (string->blob "tea")
     (generic-hash (string->blob "data") size: hash-size))
    (test '(("te" . (("a" .())
		     ("st" . (("ers" . ())
			      ("ing" . ()))))))
	  (tree->list store root-node))
    (test 5 (dup-count store))
    (test '("te") (dirty-keys store))
    (mdb-txn-commit (hash-store-txn store))))

(test-group "insert - split nested node and with existing children"
  (let ((store (make-test-store
		'(("test" . (("ers" . ())
			     ("ing" . ())))))))
    
    (hash-put
     store
     (string->blob "tested")
     (generic-hash (string->blob "data") size: hash-size))
    (test '(("test" . (("e" . (("d" . ())
    			       ("rs" . ())))
    		       ("ing" . ()))))
    	  (tree->list store root-node))
    (test 5 (dup-count store))
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
    (test 6 (dup-count store))
    (test '("teste" "test") (dirty-keys store))
    (mdb-txn-commit (hash-store-txn store))))

(test-group "insert - replace existing node"
  (let ((store (make-test-store
		'(("test" . (("ers" . ())
			     ("ing" . ())))))))
    (let ((hash2 (generic-hash (string->blob "data2") size: hash-size)))
      (hash-put
       store
       (string->blob "testers")
       hash2)
      (test '(("test" . (("ers" . ())
			 ("ing" . ()))))
	    (tree->list store root-node))
      (test hash2 (hash-get store (string->blob "testers")))
      (test 3 (dup-count store))
      (test '("test") (dirty-keys store))
      (mdb-txn-commit (hash-store-txn store)))))

(test-group "insert - split resulting in empty subkey"
  (let ((store (make-test-store '(("test" . ()))))
	(hash (generic-hash (string->blob "data") size: hash-size))
	(hash2 (generic-hash (string->blob "data2") size: hash-size)))
    (hash-put store (string->blob "tests") hash2)
    (test '(("test" . (("" . ())
		       ("s" . ()))))
	  (tree->list store root-node))
    (test hash2 (hash-get store (string->blob "tests")))
    (test hash (hash-get store (string->blob "test")))
    (test 3 (dup-count store))
    (test '("test") (dirty-keys store))))

(test-group "del - delete only root node"
  (let ((hash2 (generic-hash (string->blob "data2") size: hash-size))
	(store (make-test-store '(("test" . ())))))
      (hash-delete store (string->blob "test"))
      (test '() (tree->list store root-node))
      (test 0 (dup-count store))
      (test '() (dirty-keys store))
      (mdb-txn-commit (hash-store-txn store))))

(test-group "del - delete leaf and do not collapse node"
  (let ((hash2 (generic-hash (string->blob "data2") size: hash-size))
	(store (make-test-store '(("test" . (("ers" . ())
					     ("ing" . ())
					     ("s" . ())))))))
      (hash-delete store (string->blob "testers"))
      (test '(("test" . (("ing" . ())
			 ("s" . ()))))
	    (tree->list store root-node))
      (test 3 (dup-count store))
      (test '("test") (dirty-keys store))
      (mdb-txn-commit (hash-store-txn store))))

(test-group "del - delete leaf and collapse node"
  (let ((hash2 (generic-hash (string->blob "data2") size: hash-size))
	(store (make-test-store '(("test" . (("ers" . ())
					     ("ing" . ())))))))
      (hash-delete store (string->blob "testers"))
      (test '(("testing" . ()))
	    (tree->list store root-node))
      (test 1 (dup-count store))
      (test '() (dirty-keys store))
      (mdb-txn-commit (hash-store-txn store))))

(test-group "del - delete leaf and collapse nodes with children"
  (let ((hash2 (generic-hash (string->blob "data2") size: hash-size))
	(store (make-test-store '(("test" . (("e" . (("d" . ())
						     ("rs" . ())))
					     ("ing" . (("" . ())
						       ("s" . ())))))))))
      (hash-delete store (string->blob "testing"))
      (test '(("test" . (("e" . (("d" . ())
				 ("rs" . ())))
			 ("ings" . ()))))
	    (tree->list store root-node))
      (test 5 (dup-count store))
      (test '("test") (dirty-keys store))
      (hash-delete store (string->blob "testings"))
      (test '(("teste" . (("d" . ())
			  ("rs" . ()))))
	    (tree->list store root-node))
      (test 3 (dup-count store))
      (test '() (dirty-keys store))
      (hash-delete store (string->blob "testers"))
      (test '(("tested" . ()))
	    (tree->list store root-node))
      (test 1 (dup-count store))
      (test '() (dirty-keys store))
      (mdb-txn-commit (hash-store-txn store))))

(test-group "del - do not attempt to collapse at root"
  (let ((hash2 (generic-hash (string->blob "data2") size: hash-size))
	(store (make-test-store '(("bar" . ())
				  ("foo" . ())))))
      (hash-delete store (string->blob "foo"))
      (test '(("bar" . ()))
	    (tree->list store root-node))
      (test 1 (dup-count store))
      (test '() (dirty-keys store))))

(test-group "del - delete nested key with empty sibiling"
  (let ((store (make-test-store '())))
    (hash-put store
	      (string->blob "foo")
	      (generic-hash (string->blob "foo-data") size: hash-size))
    (hash-put store
	      (string->blob "foobar")
	      (generic-hash (string->blob "foobar-data") size: hash-size))
    (hash-delete store (string->blob "foobar"))
    (test '(("foo" . ()))
	  (tree->list store root-node))
    (test 1 (dup-count store))
    (test '() (dirty-keys store))))

(test-group "inserting then deleting keys results in empty tree"
  (let* ((store (make-test-store '()))
	 (pairs (<- (gen-list-of
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
    (test 0 (dup-count store))
    (test '() (dirty-keys store))))

(test-group "inserted hashes can be retrieved with hash-get"
   (let ((store (make-test-store '())))
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
		pairs))))

(test-group "get root hash with only top level nodes"
  (let ((store (make-test-store '()))
	(foo-hash (generic-hash (string->blob "foo") size: hash-size))
	(bar-hash (generic-hash (string->blob "bar") size: hash-size)))
    ;; empty blob hashing doesn't seem to be supported in the
    ;; lmdb version which ships with debian jessie (0.9.14-1)
    ;; (test (generic-hash (string->blob "") size: hash-size)
    ;;       (root-hash store))
    (hash-put store (string->blob "foo") foo-hash)
    (test foo-hash (root-hash store))
    (hash-put store (string->blob "bar") bar-hash)
    (test (generic-hash (blob-append bar-hash foo-hash) size: hash-size)
	  (root-hash store))))

(test-group "rehash tree + prefix-hash"
  (let ((store (make-test-store '()))
	(tested-hash (generic-hash (string->blob "tested") size: hash-size))
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
      (test '() (dirty-keys store)))))

(test-group "inserting a key changes the root hash"
  (let* ((store (make-test-store '()))
	 (pairs (<- (gen-list-of
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
      (test-assert (not (equal? hash1 (root-hash store)))))))

(test-group "deleting a key changes the root hash"
  (let* ((store (make-test-store '()))
	 (pairs (<- (gen-list-of
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
      (test-assert (not (equal? hash1 (root-hash store)))))))

(test-group "compare hash for differently built trees with same end-state"
  (let ((root-hash1 #f)
	(root-hash2 #f))
    (let ((store (make-test-store '())))
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
      (set! root-hash1 (root-hash store)))
    (let ((store (make-test-store '())))
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
      (set! root-hash2 (root-hash store)))
    (test root-hash1 (identity root-hash2))))

(test-group "random re-hash calls should not change final result"
  (let* ((store (make-test-store '()))
	 (pairs (<- (gen-list-of
		     (gen-pair-of (random-low-variation-key store)
				  (random-hash))
		     100))))
    (for-each (lambda (pair)
		(hash-put store (car pair) (cdr pair)))
	      pairs)
    (rehash store)
    (let ((hash1 (root-hash store))
	  (store (make-test-store '())))
      (for-each (lambda (pair)
		  (hash-put store (car pair) (cdr pair))
		  ;; randomly re-hash
		  (when (= 1 (random 2))
		    (rehash store)))
		pairs)
      (rehash store)
      (test hash1 (root-hash store)))))

(test-group "node-children on range bound subtree"
  (let ((store (make-test-store '(("a". ())
				  ("b" . (("ar" . ())
					  ("ee" . ())))
				  ("c" . ())
				  ("d" . ())))))
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
				    (string->u8vector "bb"))))))
  (let ((store2 (make-test-store '(("aa" . (("1" . ())
					    ("2" . ())))
				   ("bb" . (("3" . ())
					    ("4" . ())))))))
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
				    (string->u8vector "ff"))))))
  (let ((store2 (make-test-store '(("foo" . (("1" . ())
					    ("2" . ())))
				   ("bb" . (("3" . ())
					    ("4" . ())))))))
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

(test-group "range bound root-hash example"
  (let* ((store (make-test-store '()))
	 (tea-hash (generic-hash (string->blob "tea-data")
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
    (test-assert (not (blob=? hash1 (root-hash store))))))

(test-group "left hash"
  (let* ((store (make-test-store '()))
	 ;; make sure all pairs are unique, otherwise it might
	 ;; invalidate a previously recorded root hash for that range
	 (pairs (delete-duplicates
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
	(loop (cdr pairs) (cdr hashes))))))

(test-group "right hash"
  (let* ((store (make-test-store '()))
	 ;; make sure all pairs are unique, otherwise it might
	 ;; invalidate a previously recorded root hash for that range
	 (pairs (delete-duplicates
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
	(loop (cdr pairs) (cdr hashes))))))

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
       (let ((store1 (make-test-store '())))
	 (for-each (lambda (pair)
		     (hash-put store1 (car pair) (cdr pair)))
		   range-pairs)
	 (set! hash1 (root-hash store1)))
       ;; build a tree with *all* pairs, and get a range hash
       ;; then compare with previous root hash
       (let ((store2 (make-test-store '()))
	     (first-pair (first range-pairs))
	     (last-pair (last range-pairs)))
	 (for-each (lambda (pair)
		     (hash-put store2 (car pair) (cdr pair)))
		   pairs)
	 (test hash1 (root-hash store2
				(blob->u8vector (car (first range-pairs)))
				(blob->u8vector (car (last range-pairs))))))))))

(test-group "range-hash with empty subkeys"
  (let* ((store (make-test-store '()))
	 (foo-hash (generic-hash (string->blob "foo-data")
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
	 ;; (root-hash1 (generic-hash test-bucket-hash
				   ;; size: hash-size))
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
    ))

(test-group "prefix-hash"
  (let* ((store (make-test-store '()))
	 (foo-hash (generic-hash (string->blob "foo-data") size: hash-size))
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
    
    ))

(test-group "child-hashes - root with single key should not return child result"
  (let ((store (make-test-store '()))
	(foo-hash (generic-hash (string->blob "foo-data") size: hash-size)))
    (hash-put store (string->blob "foo") foo-hash)
    (test `(#(,(string->u8vector "foo") ,foo-hash #t))
	  (lazy-seq->list (child-hashes store #u8{})))))

(test-group "child-hashes"
  (let* ((store (make-test-store '()))
	 (foo-hash (generic-hash (string->blob "foo-data") size: hash-size))
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
    (test "if only one child in range, return it's children directly"
	  `(#(,(string->u8vector "tested") ,tested-hash #t)
	    #(,(string->u8vector "tester") ,tester-bucket-hash #f))
	  (lazy-seq->list
	   (child-hashes store
			 (string->u8vector "test")
			 (string->u8vector "tested")
			 (string->u8vector "testers"))))))

(test-group "leaf-hashes"
  (let* ((store (make-test-store '()))
	 (foo-hash (generic-hash (string->blob "foo-data") size: hash-size))
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
			(string->u8vector "tested"))))))


(test-group "diff - L is null, R is leaf"
  (let ((store (make-test-store)))
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (foo-hash (generic-hash (string->blob "one") size: hash-size)))
      (hash-put store2 (string->blob "foo") foo-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,foo-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is null, L is leaf"
  (let ((store (make-test-store)))
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size)))
      (hash-put store1 (string->blob "foo") one-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L < R"
  (let ((store (make-test-store)))
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (hash-put store1 (string->blob "aaa") one-hash)
      (hash-put store2 (string->blob "bbb") two-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "aaa") #f)
			  #(new ,(string->blob "bbb") ,two-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L > R"
  (let ((store (make-test-store)))
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (hash-put store2 (string->blob "aaa") one-hash)
      (hash-put store1 (string->blob "bbb") two-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "aaa") ,one-hash)
			  #(missing ,(string->blob "bbb") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is leaf, R is leaf, same hash"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size)))
    (hash-put store (string->blob "asdf") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `()
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is leaf, R is leaf, different hash"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(data2-hash (generic-hash (string->blob "data2") size: hash-size)))
    (hash-put store (string->blob "asdf") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store2 (string->blob "asdf") data2-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(different ,(string->blob "asdf") ,data2-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is null, R is not leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size)))
    (hash-put store (string->blob "asdf") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store2 (string->blob "foo") one-hash)
      (hash-put store2 (string->blob "foobar") two-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,one-hash)
			  #(new ,(string->blob "foobar") ,two-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is null, L is not leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size)))
    (hash-put store (string->blob "asdf") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store1 (string->blob "foo") one-hash)
      (hash-put store1 (string->blob "foobar") two-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f)
			  #(missing ,(string->blob "foobar") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L < R, L is not leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store1 (string->blob "foo") one-hash)
      (hash-put store1 (string->blob "foobar") two-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f)
			  #(missing ,(string->blob "foobar") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L > R, R is not leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store2 (string->blob "foo") one-hash)
      (hash-put store2 (string->blob "foobar") two-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,one-hash)
			  #(new ,(string->blob "foobar") ,two-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is not leaf, R is not leaf, same hash"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (hash-put store (string->blob "foo") one-hash)
    (hash-put store (string->blob "foobar") two-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `()
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is not leaf, R is not leaf, different hash"
  (let ((store (make-test-store))
    	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size))
	(three-hash (generic-hash (string->blob "three") size: hash-size))
	(four-hash (generic-hash (string->blob "four") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (hash-put store (string->blob "foo") one-hash)
    (hash-put store (string->blob "foobar") two-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store1 (string->blob "fool") three-hash)
      (hash-put store2 (string->blob "food") four-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "food") ,four-hash)
			  #(missing ,(string->blob "fool") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is prefix of R, L is leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size)))
      (hash-put store2 (string->blob "foobar") one-hash)
      (hash-put store1 (string->blob "foo") two-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f)
			  #(new ,(string->blob "foobar") ,one-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is prefix of R, L is not leaf, R is leaf, L does not contain R"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size))
	(three-hash (generic-hash (string->blob "three") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (three-hash (generic-hash (string->blob "three") size: hash-size)))
      (hash-put store1 (string->blob "foobar") one-hash)
      (hash-put store1 (string->blob "foobaz") two-hash)
      (hash-put store2 (string->blob "foobab") three-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foobab") ,three-hash)
			  #(missing ,(string->blob "foobar") #f)
			  #(missing ,(string->blob "foobaz") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is prefix of R, L is not leaf, R is leaf, L contains R"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store1 (string->blob "foobar") one-hash)
      (hash-put store1 (string->blob "foobaz") two-hash)
      (hash-put store2 (string->blob "foobar") one-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foobaz") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is prefix of R, L is not leaf, R is not a leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size))
	(three-hash (generic-hash (string->blob "three") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store1 (string->blob "foobar") one-hash)
      (hash-put store1 (string->blob "food") two-hash)
      (hash-put store2 (string->blob "foobar") one-hash)
      (hash-put store2 (string->blob "foobaz") three-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "food") #f)
			  #(new ,(string->blob "foobaz") ,three-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is prefix of L, R is leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store2 (string->blob "foo") two-hash)
      (hash-put store1 (string->blob "foobar") one-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,two-hash)
			  #(missing ,(string->blob "foobar") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is prefix of L, R is not leaf, L is leaf, R does not contain L"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size))
	(three-hash (generic-hash (string->blob "three") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store2 (string->blob "foobar") one-hash)
      (hash-put store2 (string->blob "foobaz") two-hash)
      (hash-put store1 (string->blob "foobab") three-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foobab") #f)
			  #(new ,(string->blob "foobar") ,one-hash)
			  #(new ,(string->blob "foobaz") ,two-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is prefix of L, R is not leaf, L is leaf, R contains L"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store2 (string->blob "foobar") one-hash)
      (hash-put store2 (string->blob "foobaz") two-hash)
      (hash-put store1 (string->blob "foobar") one-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foobaz") ,two-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is prefix of L, R is not leaf, L is not a leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size))
	(three-hash (generic-hash (string->blob "three") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (hash-put store2 (string->blob "foobar") one-hash)
      (hash-put store2 (string->blob "food") two-hash)
      (hash-put store1 (string->blob "foobar") one-hash)
      (hash-put store1 (string->blob "foobaz") three-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "food") ,two-hash)
			  #(missing ,(string->blob "foobaz") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is leaf, R is not leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size))
	(three-hash (generic-hash (string->blob "three") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store1 (string->blob "foo") one-hash)
      (hash-put store2 (string->blob "foobar") two-hash)
      (hash-put store2 (string->blob "food") three-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f)
			  #(new ,(string->blob "foobar") ,two-hash)
			  #(new ,(string->blob "food") ,three-hash))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is leaf, R is not leaf"
  (let ((store (make-test-store))
	(data-hash (generic-hash (string->blob "data") size: hash-size))
	(one-hash (generic-hash (string->blob "one") size: hash-size))
	(two-hash (generic-hash (string->blob "two") size: hash-size))
	(three-hash (generic-hash (string->blob "three") size: hash-size)))
    (hash-put store (string->blob "test") data-hash)
    (mdb-txn-commit (hash-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (hash-put store1 (string->blob "foobar") two-hash)
      (hash-put store1 (string->blob "food") three-hash)
      (hash-put store2 (string->blob "foo") one-hash)
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,one-hash)
			  #(missing ,(string->blob "foobar") #f)
			  #(missing ,(string->blob "food") #f))
			(lazy-seq->list (hash-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (hash-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

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
		     1000))))
	(store (make-test-store)))
    (for-each (lambda (pair)
		(hash-put store (car pair) (cdr pair)))
	      pairs)
    (mdb-txn-commit (hash-store-txn store))
    (mdb-env-close (mdb-txn-env (hash-store-txn store)))
    ;; test some random operations
    (test-generative
     ((operations (gen-random-operations pairs 20)))
     (clear-testdb "tests/testdb-copy")
     (store-copy-path "tests/testdb" "tests/testdb-copy")
     (let ((store1 (open-test-store "tests/testdb"))
	   (store2 (open-test-store "tests/testdb-copy")))
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
		     (test expected
			   (sort
			    (lazy-seq->list (hash-diff store1 s1-in s1-out))
			    (lambda (a b)
			      (string<? (blob->string (vector-ref a 1))
					(blob->string (vector-ref b 1))))))
		     (close-input-port s1-in)
		     (close-output-port s1-out))
		   'diff))
		 (store2-thread
		  (make-thread
		   (lambda ()
		     (hash-diff-accept store2 s2-in s2-out)
		     (close-input-port s2-in)
		     (close-output-port s2-out))
		   'accept)))
	     (thread-start! store1-thread)
	     (thread-start! store2-thread)
	     (thread-join! store1-thread)
	     (thread-join! store2-thread))))
       ;; tidy up
       (mdb-txn-commit (hash-store-txn store1))
       (mdb-txn-commit (hash-store-txn store2))
       (mdb-env-close (mdb-txn-env (hash-store-txn store1)))
       (mdb-env-close (mdb-txn-env (hash-store-txn store2)))))))

