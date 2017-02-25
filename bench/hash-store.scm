(use posix
     lmdb-lolevel
     dust.hash-store
     data-generators
     srfi-1)

(define tmp-db-path "bench/tmp-db")

(define (clear-db)
  (when (file-exists? tmp-db-path)
    (delete-directory tmp-db-path #t))
  (create-directory tmp-db-path))

(define (random-pairs n)
  (<- (gen-list-of
       (gen-pair-of
	(with-size 100
		   (gen-transform (compose u8vector->blob list->u8vector)
				  (gen-list-of (gen-uint8))))
	(with-size hash-size
		   (gen-transform (compose u8vector->blob list->u8vector)
				  (gen-list-of (gen-uint8)))))
       n)))

(print "single insert per commit")
(clear-db)
(let ((count 1000)
      (env (mdb-env-create)))
  (mdb-env-set-mapsize env 100000000)
  (mdb-env-set-maxdbs env 2)
  (mdb-env-open env tmp-db-path 0
		(bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
  (let ((pairs (random-pairs count))
	(start (current-milliseconds)))
    (for-each (lambda (pair)
		(let* ((txn (mdb-txn-begin env #f 0))
		       (store (hash-store-open txn)))
		  (hash-put store (car pair) (cdr pair))
		  (mdb-txn-commit txn)))
	      pairs)
    (let ((duration (- (current-milliseconds) start)))
      (printf "time: ~Sms~n" duration)
      (printf "      ~S/sec~n" (round (/ count (/ duration 1000)))))))

(print "single insert per commit (nosync)")
(clear-db)
(let ((count 1000)
      (env (mdb-env-create)))
  (mdb-env-set-mapsize env 100000000)
  (mdb-env-set-maxdbs env 2)
  (mdb-env-open env tmp-db-path MDB_NOSYNC
		(bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
  (let ((pairs (random-pairs count))
	(start (current-milliseconds)))
    (for-each (lambda (pair)
		(let* ((txn (mdb-txn-begin env #f 0))
		       (store (hash-store-open txn)))
		  (hash-put store (car pair) (cdr pair))
		  (mdb-txn-commit txn)))
	      pairs)
    (let ((duration (- (current-milliseconds) start)))
      (printf "time: ~Sms~n" duration)
      (printf "      ~S/sec~n" (round (/ count (/ duration 1000)))))))

(define (group lst size)
  (if (null? lst)
      '()
      (cons (take lst size)
	    (group (drop lst size) size))))

(print "multiple inserts per commit (10)")
(clear-db)
(let ((count 1000)
      (env (mdb-env-create)))
  (mdb-env-set-mapsize env 100000000)
  (mdb-env-set-maxdbs env 2)
  (mdb-env-open env tmp-db-path 0
		(bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
  (let* ((pairs (random-pairs count))
	 (groups (group pairs 10))
	 (start (current-milliseconds)))
    (for-each (lambda (group)
		(let* ((txn (mdb-txn-begin env #f 0))
		       (store (hash-store-open txn)))
		  (for-each (lambda (pair)
			      (hash-put store (car pair) (cdr pair)))
			    group)
		  (mdb-txn-commit txn)))
	      groups)
    (let ((duration (- (current-milliseconds) start)))
      (printf "time: ~Sms~n" duration)
      (printf "      ~S/sec~n" (round (/ count (/ duration 1000)))))))

(print "multiple inserts per commit (100)")
(clear-db)
(let ((count 1000)
      (env (mdb-env-create)))
  (mdb-env-set-mapsize env 100000000)
  (mdb-env-set-maxdbs env 2)
  (mdb-env-open env tmp-db-path 0
		(bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
  (let* ((pairs (random-pairs count))
	 (groups (group pairs 100))
	 (start (current-milliseconds)))
    (for-each (lambda (group)
		(let* ((txn (mdb-txn-begin env #f 0))
		       (store (hash-store-open txn)))
		  (for-each (lambda (pair)
			      (hash-put store (car pair) (cdr pair)))
			    group)
		  (mdb-txn-commit txn)))
	      groups)
    (let ((duration (- (current-milliseconds) start)))
      (printf "time: ~Sms~n" duration)
      (printf "      ~S/sec~n" (round (/ count (/ duration 1000)))))))


(print "single insert per commit (+ rehash)")
(clear-db)
(let ((count 1000)
      (env (mdb-env-create)))
  (mdb-env-set-mapsize env 100000000)
  (mdb-env-set-maxdbs env 2)
  (mdb-env-open env tmp-db-path 0
        (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
  (let ((pairs (random-pairs count))
    (start (current-milliseconds)))
    (for-each (lambda (pair)
        (let* ((txn (mdb-txn-begin env #f 0))
               (store (hash-store-open txn)))
          (hash-put store (car pair) (cdr pair))
          (rehash store)
          (mdb-txn-commit txn)))
          pairs)
    (let ((duration (- (current-milliseconds) start)))
      (printf "time: ~Sms~n" duration)
      (printf "      ~S/sec~n" (round (/ count (/ duration 1000)))))))

(print "multiple inserts per commit (100) (+ rehash)")
(clear-db)
(let ((count 1000)
      (env (mdb-env-create)))
  (mdb-env-set-mapsize env 100000000)
  (mdb-env-set-maxdbs env 2)
  (mdb-env-open env tmp-db-path 0
        (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
  (let* ((pairs (random-pairs count))
     (groups (group pairs 100))
     (start (current-milliseconds)))
    (for-each (lambda (group)
        (let* ((txn (mdb-txn-begin env #f 0))
               (store (hash-store-open txn)))
          (for-each (lambda (pair)
                  (hash-put store (car pair) (cdr pair)))
                group)
          (rehash store)
          (mdb-txn-commit txn)))
          groups)
    (let ((duration (- (current-milliseconds) start)))
      (printf "time: ~Sms~n" duration)
      (printf "      ~S/sec~n" (round (/ count (/ duration 1000)))))))
