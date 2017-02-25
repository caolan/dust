(use test
     test-generative
     data-generators
     posix
     srfi-1
     lmdb-lolevel
     matchable
     sodium
     lazy-seq
     unix-sockets
     dust.hash-store
     dust.kv-store)

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

(define (reopen-store store)
  (let* ((env (mdb-txn-env (kv-store-txn store)))
	 (new-txn (mdb-txn-begin env #f 0)))
    (kv-store-open new-txn)))

(define (make-test-store)
  (let ((path "tests/testdb"))
    (clear-testdb path)
    (open-test-store path)))

(define (make-copy-store store)
  (let ((path "tests/testdb-copy"))
    (clear-testdb path)
    (mdb-env-copy (mdb-txn-env (kv-store-txn store)) path)
    (open-test-store path)))

(define (store-copy-path from to)
  (let* ((env (open-test-env from)))
    (mdb-env-copy env to)
    (mdb-env-close env)))

(test-group "simple put and get"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "foo") (string->blob "bar"))
    (test (string->blob "bar")
	  (kv-get store (string->blob "foo")))
    (test (generic-hash (string->blob "bar") size: hash-size)
	  (hash-get (kv-store-hash-store store) (string->blob "foo")))
    ))

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

(test-group "diff - L is null, R is leaf"
  (let ((store (make-test-store)))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (foo-hash (generic-hash (string->blob "one") size: hash-size)))
      (kv-put store2 (string->blob "foo") (string->blob "one"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,foo-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is null, L is leaf"
  (let ((store (make-test-store)))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (kv-put store1 (string->blob "foo") (string->blob "one"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L < R"
  (let ((store (make-test-store)))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store1 (string->blob "aaa") (string->blob "one"))
      (kv-put store2 (string->blob "bbb") (string->blob "two"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "aaa") #f)
			  #(new ,(string->blob "bbb") ,two-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L > R"
  (let ((store (make-test-store)))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store2 (string->blob "aaa") (string->blob "one"))
      (kv-put store1 (string->blob "bbb") (string->blob "two"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "aaa") ,one-hash)
			  #(missing ,(string->blob "bbb") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is leaf, R is leaf, same hash"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "asdf") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `()
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is leaf, R is leaf, different hash"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "asdf") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (data2-hash (generic-hash (string->blob "data2") size: hash-size)))
      (kv-put store2 (string->blob "asdf") (string->blob "data2"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(different ,(string->blob "asdf") ,data2-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is null, R is not leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "asdf") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store2 (string->blob "foo") (string->blob "one"))
      (kv-put store2 (string->blob "foobar") (string->blob "two"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,one-hash)
			  #(new ,(string->blob "foobar") ,two-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is null, L is not leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "asdf") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store1 (string->blob "foo") (string->blob "one"))
      (kv-put store1 (string->blob "foobar") (string->blob "two"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f)
			  #(missing ,(string->blob "foobar") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L < R, L is not leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store1 (string->blob "foo") (string->blob "one"))
      (kv-put store1 (string->blob "foobar") (string->blob "two"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f)
			  #(missing ,(string->blob "foobar") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L > R, R is not leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store2 (string->blob "foo") (string->blob "one"))
      (kv-put store2 (string->blob "foobar") (string->blob "two"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,one-hash)
			  #(new ,(string->blob "foobar") ,two-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is not leaf, R is not leaf, same hash"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (kv-put store (string->blob "foo") (string->blob "one"))
    (kv-put store (string->blob "foobar") (string->blob "two"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `()
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is not leaf, R is not leaf, different hash"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (kv-put store (string->blob "foo") (string->blob "one"))
    (kv-put store (string->blob "foobar") (string->blob "two"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (three-hash (generic-hash (string->blob "three") size: hash-size))
	  (four-hash (generic-hash (string->blob "four") size: hash-size)))
      (kv-put store1 (string->blob "fool") (string->blob "three"))
      (kv-put store2 (string->blob "food") (string->blob "four"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "food") ,four-hash)
			  #(missing ,(string->blob "fool") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is prefix of R, L is leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size)))
      (kv-put store2 (string->blob "foobar") (string->blob "one"))
      (kv-put store1 (string->blob "foo") (string->blob "two"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f)
			  #(new ,(string->blob "foobar") ,one-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is prefix of R, L is not leaf, R is leaf, L does not contain R"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (three-hash (generic-hash (string->blob "three") size: hash-size)))
      (kv-put store1 (string->blob "foobar") (string->blob "one"))
      (kv-put store1 (string->blob "foobaz") (string->blob "two"))
      (kv-put store2 (string->blob "foobab") (string->blob "three"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foobab") ,three-hash)
			  #(missing ,(string->blob "foobar") #f)
			  #(missing ,(string->blob "foobaz") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is prefix of R, L is not leaf, R is leaf, L contains R"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store)))
      (kv-put store1 (string->blob "foobar") (string->blob "one"))
      (kv-put store1 (string->blob "foobaz") (string->blob "two"))
      (kv-put store2 (string->blob "foobar") (string->blob "one"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foobaz") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L is prefix of R, L is not leaf, R is not a leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (three-hash (generic-hash (string->blob "three") size: hash-size)))
      (kv-put store1 (string->blob "foobar") (string->blob "one"))
      (kv-put store1 (string->blob "food") (string->blob "two"))
      (kv-put store2 (string->blob "foobar") (string->blob "one"))
      (kv-put store2 (string->blob "foobaz") (string->blob "three"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "food") #f)
			  #(new ,(string->blob "foobaz") ,three-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))


(test-group "diff - R is prefix of L, R is leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store2 (string->blob "foo") (string->blob "two"))
      (kv-put store1 (string->blob "foobar") (string->blob "one"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,two-hash)
			  #(missing ,(string->blob "foobar") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is prefix of L, R is not leaf, L is leaf, R does not contain L"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store2 (string->blob "foobar") (string->blob "one"))
      (kv-put store2 (string->blob "foobaz") (string->blob "two"))
      (kv-put store1 (string->blob "foobab") (string->blob "three"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foobab") #f)
			  #(new ,(string->blob "foobar") ,one-hash)
			  #(new ,(string->blob "foobaz") ,two-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is prefix of L, R is not leaf, L is leaf, R contains L"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store2 (string->blob "foobar") (string->blob "one"))
      (kv-put store2 (string->blob "foobaz") (string->blob "two"))
      (kv-put store1 (string->blob "foobar") (string->blob "one"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foobaz") ,two-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - R is prefix of L, R is not leaf, L is not a leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (two-hash (generic-hash (string->blob "two") size: hash-size)))
      (kv-put store2 (string->blob "foobar") (string->blob "one"))
      (kv-put store2 (string->blob "food") (string->blob "two"))
      (kv-put store1 (string->blob "foobar") (string->blob "one"))
      (kv-put store1 (string->blob "foobaz") (string->blob "three"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "food") ,two-hash)
			  #(missing ,(string->blob "foobaz") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is leaf, R is not leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (two-hash (generic-hash (string->blob "two") size: hash-size))
	  (three-hash (generic-hash (string->blob "three") size: hash-size)))
      (kv-put store1 (string->blob "foo") (string->blob "one"))
      (kv-put store2 (string->blob "foobar") (string->blob "two"))
      (kv-put store2 (string->blob "food") (string->blob "three"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(missing ,(string->blob "foo") #f)
			  #(new ,(string->blob "foobar") ,two-hash)
			  #(new ,(string->blob "food") ,three-hash))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
		'accept)))
	  (thread-start! store1-thread)
	  (thread-start! store2-thread)
	  (thread-join! store1-thread)
	  (thread-join! store2-thread))))))

(test-group "diff - L == R, L is leaf, R is not leaf"
  (let ((store (make-test-store)))
    (kv-put store (string->blob "test") (string->blob "data"))
    (mdb-txn-commit (kv-store-txn store))
    (let ((store2 (make-copy-store store))
	  (store1 (reopen-store store))
	  (one-hash (generic-hash (string->blob "one") size: hash-size)))
      (kv-put store1 (string->blob "foobar") (string->blob "two"))
      (kv-put store1 (string->blob "food") (string->blob "three"))
      (kv-put store2 (string->blob "foo") (string->blob "one"))
      (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
	(let ((store1-thread
	       (make-thread
		(lambda ()
		  (test `(#(new ,(string->blob "foo") ,one-hash)
			  #(missing ,(string->blob "foobar") #f)
			  #(missing ,(string->blob "food") #f))
			(lazy-seq->list (kv-diff store1 s1-in s1-out)))
		  (close-input-port s1-in)
		  (close-output-port s1-out))
		'diff))
	      (store2-thread
	       (make-thread
		(lambda ()
		  (kv-diff-accept store2 s2-in s2-out))
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
		(kv-put store (car pair) (cdr pair)))
	      pairs)
    (mdb-txn-commit (kv-store-txn store))
    (mdb-env-close (mdb-txn-env (kv-store-txn store)))
    ;; test some random operations
    (test-generative
     ((operations (gen-random-operations pairs 20)))
     (clear-testdb "tests/testdb-copy")
     (store-copy-path "tests/testdb" "tests/testdb-copy")
     (let ((store1 (open-test-store "tests/testdb"))
	   (store2 (open-test-store "tests/testdb-copy")))
       (for-each
	(match-lambda
	  (('create key value)
	   (kv-put store2 key value))
	  (('update key value)
	   (kv-put store2 key value))
	  (('delete key)
	   (kv-delete store2 key)))
	operations)
       (let ((expected
	      (sort
	       (map (match-lambda
			(('create key value)
			 (vector 'new
				 key
				 (generic-hash value size: hash-size)))
		      (('update key value)
		       (vector 'different
			       key
			       (generic-hash value size: hash-size)))
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
			    (lazy-seq->list (kv-diff store1 s1-in s1-out))
			    (lambda (a b)
			      (string<? (blob->string (vector-ref a 1))
					(blob->string (vector-ref b 1))))))
		     (close-input-port s1-in)
		     (close-output-port s1-out))
		   'diff))
		 (store2-thread
		  (make-thread
		   (lambda ()
		     (kv-diff-accept store2 s2-in s2-out)
		     (close-input-port s2-in)
		     (close-output-port s2-out))
		   'accept)))
	     (thread-start! store1-thread)
	     (thread-start! store2-thread)
	     (thread-join! store1-thread)
	     (thread-join! store2-thread))))
       ;; tidy up
       (mdb-txn-commit (kv-store-txn store1))
       (mdb-txn-commit (kv-store-txn store2))
       (mdb-env-close (mdb-txn-env (kv-store-txn store1)))
       (mdb-env-close (mdb-txn-env (kv-store-txn store2)))))))

