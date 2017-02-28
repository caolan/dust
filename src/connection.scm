(module dust.connection *

(import chicken scheme)
(use srfi-18 srfi-117 bencode)

(define-record connection in out pending)

(define (connection in out)
  (make-connection in out (list-queue)))

(define (close-connection conn)
  (close-input-port (connection-in conn))
  (close-output-port (connection-out conn)))

(define (receive-bencode conn)
  (let ((data (read-bencode (connection-in conn))))
    ;; (printf "~S received ~S~n" (current-thread) data)
    data))

(define (with-connection in out thunk)
  (let ((conn (connection in out)))
    (handle-exceptions exn (begin
			     (close-connection conn)
			     (abort exn))
		       (thunk conn))
    (close-connection conn)))

)
