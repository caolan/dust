(module dust.client

(connect
 command
 ping)

(import chicken scheme foreign)

(use unix-sockets
     bencode
     posix
     extras
     pathname-expand
     dust.connection)

(define (connect path)
  (receive (in out) (unix-connect (pathname-expand path))
    (connection in out)))

(define (ping conn)
  (write-bencode '((method . "ping")) (connection-out conn))
  (receive-bencode conn))

(define (command conn method)
  (write-bencode `((method . ,method)) (connection-out conn))
  (receive-bencode conn))

)
