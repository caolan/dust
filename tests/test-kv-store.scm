(use test
     test-generative
     data-generators
     posix
     lazy-seq
     lmdb-lolevel
     sodium
     srfi-18
     matchable
     unix-sockets
     dust.connection
     dust.hash-store
     dust.kv-store)

(define (clear-testdb path)
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
    (kv-store-open txn)))

(define (make-test-store #!optional (path "tests/testdb"))
  (clear-testdb path)
  (open-test-store path))

(define (store-copy-path from to)
  (let* ((env (open-test-env from)))
    (mdb-env-copy env to)
    (mdb-env-close env)))


;; produces a random key 20 bytes long where each byte is in the range
;; 0-2 to encourage clashes and nesting
(define (random-low-variation-key store #!optional size)
  (with-size
   (or size
       (range 20 (hash-store-max-key-size
		  (mdb-txn-env (hash-store-txn store)))))
   (gen-transform (compose u8vector->blob list->u8vector)
		  (gen-list-of (gen-fixnum 0 2)))))

(define (random-data n)
  (with-size n
	     (gen-transform (compose u8vector->blob list->u8vector)
			    (gen-list-of (gen-uint8)))))




(test-group "simple put and get"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "foo") (string->blob "bar"))
    (test (string->blob "bar")
	  (kv-get store (string->blob "foo")))
    (test (generic-hash (string->blob "bar") size: hash-size)
	  (hash-get (kv-store-hash-store store) (string->blob "foo")))
    ))

(test-group "kv-pairs"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "foo") (string->blob "one"))
    (kv-put store (string->blob "bar") (string->blob "two"))
    (kv-put store (string->blob "baz") (string->blob "three"))
    (test `((,(string->blob "bar") . ,(string->blob "two"))
	    (,(string->blob "baz") . ,(string->blob "three"))
	    (,(string->blob "foo") . ,(string->blob "one")))
	  (lazy-seq->list (kv-pairs store)))))

(test-group "get after delete"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "foo") (string->blob "bar"))
    (kv-put store (string->blob "baz") (string->blob "qux"))
    (test (string->blob "bar")
	  (kv-get store (string->blob "foo")))
    (kv-delete store (string->blob "foo"))
    (test-error (kv-get store (string->blob "foo")))))

(test-group "inserting a key changes root hash"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "foo") (string->blob "bar"))
    (let ((hash1 (root-hash (kv-store-hash-store store))))
      (kv-put store (string->blob "baz") (string->blob "qux"))
      (test-assert
	  (not (equal? hash1 (root-hash (kv-store-hash-store store))))))))

(test-group "deleting a key changes root hash"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "foo") (string->blob "bar"))
    (kv-put store (string->blob "baz") (string->blob "qux"))
    (let ((hash1 (root-hash (kv-store-hash-store store))))
      (kv-delete store (string->blob "baz"))
      (test-assert
	  (not (equal? hash1 (root-hash (kv-store-hash-store store))))))))

(test-group "updating a key changes root hash"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "foo") (string->blob "bar"))
    (let ((hash1 (root-hash (kv-store-hash-store store))))
      (kv-put store (string->blob "foo") (string->blob "asdf"))
      (test-assert
	  (not (equal? hash1 (root-hash (kv-store-hash-store store))))))))



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
	   (loop (cons `(create ,(<- (random-low-variation-key #f 8))
				,(make-blob 12))
		       ops)
		 (- n 1)))
	  ((1) ;; update
	   (let ((pair (select-pair-not-in-operations pairs ops)))
	     (loop (cons `(update ,(car pair)
				  ,(make-blob 12))
			 ops)
		   (- n 1))))
	  ((2) ;; delete
	   (let ((pair (select-pair-not-in-operations pairs ops)))
	     (loop (cons `(delete ,(car pair))
			 ops)
		   (- n 1))))))))

(test-group "sync"
  (test-generative
   ((params
     (gen-transform
      (lambda (pairs)
	(list pairs (<- (gen-random-operations pairs 25))))
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
		     (random-data 100))
	100)))))
   (receive (pairs operations) (apply values params)
     (clear-testdb "tests/testdb")
     (clear-testdb "tests/testdb2")
     (let ((env1 (open-test-env "tests/testdb"))
	   (env2 (open-test-env "tests/testdb2")))
       (with-kv-store env1 0
	(lambda (write-store1)
	  (with-kv-store env2 0
	   (lambda (write-store2)
	     ;; pre-load databases
	     (for-each (lambda (pair)
			 (kv-put write-store1 (car pair) (cdr pair))
			 (kv-put write-store2 (car pair) (cdr pair)))
		       pairs)
	     (for-each
	      (match-lambda
		  (('create key value)
		   (kv-put write-store2 key value))
		(('update key value)
		 (kv-put write-store2 key value))
		(('delete key)
		 (kv-delete write-store2 key)))
	      operations)
	     (test-assert "root hashes are different at start"
	       (not (equal? (root-hash (kv-store-hash-store write-store1))
			    (root-hash (kv-store-hash-store write-store2)))))))))
       (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	 (let ((store1-thread
		(make-thread
		 (lambda ()
		   (with-connection
		    s1-in
		    s1-out
		    (lambda (conn)
		      (kv-env-sync env1 conn))))
		 'local))
	       (store2-thread
		(make-thread
		 (lambda ()
		   (with-connection
		    s2-in
		    s2-out
		    (lambda (conn)
		      (kv-env-sync-accept env2 conn))))
		 'remote)))
	   (thread-start! store1-thread)
	   (thread-start! store2-thread)
	   (thread-join! store1-thread)
	   (thread-join! store2-thread)))
       ;; TODO: test all keys/values match up using (kv-stream)?
       (with-kv-store env1 0
	(lambda (write-store1)
	  (with-kv-store env2 0
	   (lambda (write-store2)
	     (test "root hashes are same after sync"
		   (root-hash (kv-store-hash-store write-store1))
		   (root-hash (kv-store-hash-store write-store2)))))))
       ;; tidy up
       (mdb-env-close env1)
       (mdb-env-close env2)))))
