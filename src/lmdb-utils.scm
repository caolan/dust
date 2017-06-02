(module dust.lmdb-utils

;;;;; Exports ;;;;;
(with-environment
 with-write-transaction
 ;;with-read-transaction
 with-cursor
 dup-values
 dup-count
 mdb-cursor-get/default
 mdb-exists?
 keys
 all-values)

(import chicken scheme)
(use lmdb-lolevel lazy-seq srfi-18 srfi-69 miscmacros posix)

(define mutexes
  (make-hash-table test: equal?
                   hash: equal?-hash))

(define (with-write-lock env thunk)
  (unless (hash-table-exists? mutexes env)
    (hash-table-set! mutexes env (make-mutex)))
  (mutex-lock! (hash-table-ref mutexes env))
  (handle-exceptions exn
    (begin
      (mutex-unlock! (hash-table-ref mutexes env))
      (abort exn))
    (begin0
     (thunk)
     (mutex-unlock! (hash-table-ref mutexes env)))))

;; waits for a write transaction to become available without blocking
;; other srfi-18 threads and ensures transaction is committed or
;; aborted once thunk has completed
(define (with-write-transaction env thunk)
  (with-write-lock
   env
   (lambda ()
     (let ((txn (mdb-txn-begin env #f 0)))
       (handle-exceptions exn
         (begin
           (mdb-txn-abort txn)
           (abort exn))
         (begin0
          (thunk txn)
          (mdb-txn-commit txn)))))))


;; TODO: update with-read-transaction to use with-write-transaction on
;; OpenBSD and a proper MDB_RDONLY transaction elsewhere?

;; ;; with-read-transaction does not require a mutex as read transactions
;; ;; can be opened concurrently. NOTE: read-only transactions currently
;; ;; not supported on OpenBSD (6.1)
;; (define (with-read-transaction env thunk)
;;   (let ((txn (mdb-txn-begin env #f MDB_RDONLY)))
;;     (handle-exceptions exn
;;       (begin
;;         (mdb-txn-abort txn)
;;         (abort exn))
;;       (begin0
;;        (thunk txn)
;;        (mdb-txn-commit txn)))))

(define (with-cursor txn dbi thunk)
  (let ((cursor (mdb-cursor-open txn dbi)))
    (handle-exceptions exn
      (begin
        (mdb-cursor-close cursor)
        (abort exn))
      (begin0
          (thunk cursor)
        (mdb-cursor-close cursor)))))

;; a more convenient way to open an LMDB environment
(define (with-environment path thunk #!key
                          (create #t)
                          mapsize
                          maxdbs
                          maxreaders
                          flags
                          (mode (bitwise-ior
                                 perm/irusr
                                 perm/iwusr
                                 perm/irgrp
                                 perm/iroth)))
  (when create (create-directory path #t))
  (let ((env (mdb-env-create)))
    (when mapsize (mdb-env-set-mapsize env mapsize))
    (when maxdbs (mdb-env-set-maxdbs env maxdbs))
    (when maxreaders (mdb-env-set-maxreaders env maxreaders))
    (handle-exceptions exn
      (begin
        (when (hash-table-exists? mutexes env)
          (hash-table-delete! mutexes env))
        (mdb-env-close env)
        (abort exn))
      (begin0
        (mdb-env-open env path flags mode)
        (thunk env)
        (when (hash-table-exists? mutexes env)
          ;; wait for writes to finish
          (mutex-lock! (hash-table-ref mutexes env))
          (hash-table-delete! mutexes env))
        (mdb-env-close env)))))

;; returns false if next value not found
(define (cursor-iter-dup cursor key first)
  ;; (printf "cursor-iter-dup: ~S ~S ~S~n" cursor key first)
  (condition-case
      (begin
        (if first
            (mdb-cursor-get cursor key #f MDB_SET_KEY)
            (mdb-cursor-get cursor #f #f MDB_NEXT_DUP))
        (mdb-cursor-data cursor))
    ((exn lmdb MDB_NOTFOUND) #f)))

;; TODO: look at (keys ...) implementation and consider rewriting this? is cursor-iter-dup used elsewhere?
(define (dup-values txn dbi key)
  ;; (printf "dup-values: ~S ~S ~S~n" txn dbi key)
  (let loop ((cursor (mdb-cursor-open txn dbi))
             (first #t))
    (or (and-let* ((data (cursor-iter-dup cursor key first)))
          (lazy-seq (cons data (loop cursor #f))))
        lazy-null)))

(define (dup-count txn dbi key)
  (condition-case
      (let ((cursor (mdb-cursor-open txn dbi)))
        (mdb-cursor-get cursor key #f MDB_SET_KEY)
        (mdb-cursor-count cursor))
    ((exn lmdb MDB_NOTFOUND) 0)))

(define (mdb-cursor-get/default cursor key data op default)
  (condition-case
      (mdb-cursor-get cursor key data op)
    ((exn lmdb MDB_NOTFOUND) default)))

(define (mdb-exists? txn dbi key)
  (condition-case
      (begin (mdb-get txn dbi key) #t)
    ((exn lmdb MDB_NOTFOUND) #f)))

(define (keys txn dbi)
  (let ((cursor (mdb-cursor-open txn dbi)))
    (let loop ((op MDB_FIRST))
      (if (condition-case
              (begin
                (mdb-cursor-get cursor #f #f op)
                #t)
            ((exn lmdb MDB_NOTFOUND) #f))
          (lazy-seq (cons (mdb-cursor-key cursor) (loop MDB_NEXT_NODUP)))
          lazy-null))))

;; includes all duplicates for all keys
(define (all-values txn dbi)
  (let ((cursor (mdb-cursor-open txn dbi)))
    (let loop ((op MDB_FIRST))
      (if (condition-case
              (begin
                (mdb-cursor-get cursor #f #f op)
                #t)
            ((exn lmdb MDB_NOTFOUND) #f))
          (lazy-seq (cons (mdb-cursor-data cursor) (loop MDB_NEXT)))
          lazy-null))))

)
