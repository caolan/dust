(use test
     test-generative
     data-generators
     lmdb-lolevel
     lazy-seq
     unix-sockets
     dust.connection
     dust.event-store
     dust.kv-store
     dust.hash-store)

(define (open-test-env path)
  (let ((env (mdb-env-create)))
    (mdb-env-set-mapsize env 10000000)
    (mdb-env-set-maxdbs env 4)
    (mdb-env-open env path MDB_NOSYNC
                  (bitwise-ior perm/irusr perm/iwusr perm/irgrp perm/iroth))
    env))

(define (clear-env env)
  (let ((txn (mdb-txn-begin env #f 0)))
    (mdb-drop txn (kv-dbi-open txn) 0)
    (mdb-drop txn (hashes-dbi-open txn) 0)
    (mdb-drop txn (dirty-dbi-open txn) 0)
    (mdb-drop txn (meta-dbi-open txn) 0)
    (mdb-txn-commit txn)))

(define (random-data n)
  (with-size n
             (gen-transform (compose u8vector->blob list->u8vector)
                            (gen-list-of (gen-uint8)))))

(test-group "add events and read back"
  (let ((env (open-test-env "tests/testdb")))
    (test-generative
     ((data (gen-list-of (random-data (range 1 100)) (range 1 100))))
     (clear-env env)
     (with-event-store
      env 0
      (lambda (write-store)
        ;; put events
        (for-each
         (lambda (event)
           (event-put write-store event))
         data)))
     (with-event-store
      env MDB_RDONLY
      (lambda (read-store)
        ;; read events
        (test "read events back"
              data
              (lazy-seq->list (events read-store))))))))

(define (sync-stores from to)
  (let-values (((s1-in s1-out s2-in s2-out) (unix-pair)))
    (let ((from-thread
           (make-thread
            (lambda ()
              (with-connection
               s1-in
               s1-out
               (lambda (conn)
                 (event-store-env-sync to conn))))
            'to))
          (to-thread
           (make-thread
            (lambda ()
              (with-connection
               s2-in
               s2-out
               (lambda (conn)
                 (event-store-env-sync-accept from conn))))
            'from)))
      (thread-start! to-thread)
      (thread-start! from-thread)
      (thread-join! to-thread)
      (thread-join! from-thread))))

(test-group "sync to empty store"
  (let ((env1 (open-test-env "tests/testdb"))
        (env2 (open-test-env "tests/testdb2")))
    (clear-env env1)
    (clear-env env2)
    (with-event-store
      env1 0
      (lambda (write-store1)
        ;; put events
        (event-put write-store1 (string->blob "one"))
        (event-put write-store1 (string->blob "two"))))
    (sync-stores env1 env2)
    (with-event-store
     env1 MDB_RDONLY
     (lambda (read-store1)
       (with-event-store
        env2 MDB_RDONLY
        (lambda (read-store2)
          (test "events sync'd to second store"
                (list (cons (event-time->blob 1 (event-store-id read-store1))
                            (string->blob "one"))
                      (cons (event-time->blob 2 (event-store-id read-store1))
                            (string->blob "two")))
                (lazy-seq->list (event-pairs read-store2)))
          (test "clocks of both stores now match"
                (event-store-clock read-store1)
                (event-store-clock read-store2))))))))

(test-group "sync from empty store (events can be added in either direction)"
  (let ((env1 (open-test-env "tests/testdb"))
        (env2 (open-test-env "tests/testdb2")))
    (clear-env env1)
    (clear-env env2)
    (with-event-store
      env2 0
      (lambda (write-store2)
        ;; put events
        (event-put write-store2 (string->blob "one"))
        (event-put write-store2 (string->blob "two"))))
    (sync-stores env1 env2)
    (with-event-store
     env1 MDB_RDONLY
     (lambda (read-store1)
       (with-event-store
        env2 MDB_RDONLY
        (lambda (read-store2)
          (test "events sync'd to first store"
                (list (cons (event-time->blob 1 (event-store-id read-store2))
                            (string->blob "one"))
                      (cons (event-time->blob 2 (event-store-id read-store2))
                            (string->blob "two")))
                (lazy-seq->list (event-pairs read-store1)))
          (test "clocks of both stores now match"
                (event-store-clock read-store1)
                (event-store-clock read-store2))))))))

(test-group "sync non-interleaved events"
  (let ((env1 (open-test-env "tests/testdb"))
        (env2 (open-test-env "tests/testdb2")))
    (clear-env env1)
    (clear-env env2)
    (with-event-store
      env1 0
      (lambda (write-store1)
        ;; put events
        (event-put write-store1 (string->blob "one"))
        (event-put write-store1 (string->blob "two"))))
    (sync-stores env1 env2)
    (with-event-store
      env2 0
      (lambda (write-store2)
        ;; put events
        (event-put write-store2 (string->blob "three"))
        (event-put write-store2 (string->blob "four"))))
    (sync-stores env1 env2)
    (with-event-store
      env1 0
      (lambda (write-store1)
        ;; put events
        (event-put write-store1 (string->blob "five"))))
    (sync-stores env1 env2)
    (with-event-store
     env1 MDB_RDONLY
     (lambda (read-store1)
       (with-event-store
        env2 MDB_RDONLY
        (lambda (read-store2)
          (let ((data (sort
                       (list (cons (event-time->blob 1 (event-store-id read-store1))
                                   (string->blob "one"))
                             (cons (event-time->blob 2 (event-store-id read-store1))
                                   (string->blob "two"))
                             (cons (event-time->blob 3 (event-store-id read-store2))
                                   (string->blob "three"))
                             (cons (event-time->blob 4 (event-store-id read-store2))
                                   (string->blob "four"))
                             (cons (event-time->blob 5 (event-store-id read-store1))
                                   (string->blob "five")))
                       (lambda (a b)
                         (string<? (blob->string (car a))
                                   (blob->string (car b)))))))
            (test "events in store1"
                  data
                  (lazy-seq->list (event-pairs read-store1)))
            (test "events in store2"
                  data
                  (lazy-seq->list (event-pairs read-store2))))))))))

(test-group "sync interleaved events"
  (let ((env1 (open-test-env "tests/testdb"))
        (env2 (open-test-env "tests/testdb2")))
    (clear-env env1)
    (clear-env env2)
    (with-event-store
      env1 0
      (lambda (write-store1)
        ;; put events
        (event-put write-store1 (string->blob "one"))
        (event-put write-store1 (string->blob "two"))))
    (sync-stores env1 env2)
    (with-event-store
      env1 0
      (lambda (write-store1)
        ;; put events
        (event-put write-store1 (string->blob "three-1"))
        (event-put write-store1 (string->blob "four-1"))))
    (with-event-store
      env2 0
      (lambda (write-store2)
        ;; put events
        (event-put write-store2 (string->blob "three-2"))))
    (sync-stores env1 env2)
    (with-event-store
     env1 MDB_RDONLY
     (lambda (read-store1)
       (with-event-store
        env2 MDB_RDONLY
        (lambda (read-store2)
          (let ((data (sort
                       (list (cons (event-time->blob 1 (event-store-id read-store1))
                                   (string->blob "one"))
                             (cons (event-time->blob 2 (event-store-id read-store1))
                                   (string->blob "two"))
                             (cons (event-time->blob 3 (event-store-id read-store1))
                                   (string->blob "three-1"))
                             (cons (event-time->blob 3 (event-store-id read-store2))
                                   (string->blob "three-2"))
                             (cons (event-time->blob 4 (event-store-id read-store1))
                                   (string->blob "four-1")))
                       (lambda (a b)
                         (string<? (blob->string (car a))
                                   (blob->string (car b)))))))
            (test "events in store1"
                  data
                  (lazy-seq->list (event-pairs read-store1)))
            (test "events in store1"
                  data
                  (lazy-seq->list (event-pairs read-store2))))))))))

(test-exit)
