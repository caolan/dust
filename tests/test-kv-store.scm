(use test
     test-generative
     data-generators
     posix
     lmdb-lolevel
     sodium
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

(define (make-test-store)
  (let ((path "tests/testdb"))
    (clear-testdb path)
    (open-test-store path)))

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
