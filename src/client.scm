(module dust.client

(connect
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

)
