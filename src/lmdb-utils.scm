(module dust.lmdb-utils

;;;;; Exports ;;;;;
(with-transaction
 dup-values
 dup-count)

(import chicken scheme)
(use lmdb-lolevel lazy-seq)

(define (with-transaction env thunk)
  (let ((txn (mdb-txn-begin env #f 0)))
    (handle-exceptions exn
      (begin
        (mdb-txn-abort txn)
        (abort exn))
      (thunk txn)
      (mdb-txn-commit txn))))

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

)

