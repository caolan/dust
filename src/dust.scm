(import chicken scheme)

(use dust.connection
     dust.client)

(define conn
  (connect "~/.dust/dust.sock"))

(pp (ping conn))
(close-connection conn)
