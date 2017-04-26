(module dust.lmdb-utils

;;;;; Exports ;;;;;
(with-transaction
 with-cursor
 dup-values
 dup-count
 mdb-cursor-get/default
 keys
 all-values)

(import chicken scheme)
(use lmdb-lolevel lazy-seq miscmacros)

(define (with-transaction env thunk)
  (let ((txn (mdb-txn-begin env #f 0)))
    (handle-exceptions exn
      (begin
        (mdb-txn-abort txn)
        (abort exn))
      (begin0
          (thunk txn)
        (mdb-txn-commit txn)))))

(define (with-cursor txn dbi thunk)
  (let ((cursor (mdb-cursor-open txn dbi)))
    (handle-exceptions exn
      (begin
        (mdb-cursor-close cursor)
        (abort exn))
      (begin0
          (thunk cursor)
        (mdb-cursor-close cursor)))))

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
  (let ((cursor (mdb-cursor-open txn dbi)))
    (mdb-cursor-get cursor key #f MDB_SET_KEY)
    (mdb-cursor-count cursor)))

(define (mdb-cursor-get/default cursor key data op default)
  (condition-case
      (mdb-cursor-get cursor key data op)
    ((exn lmdb MDB_NOTFOUND) default)))

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
