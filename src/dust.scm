(import chicken scheme)

(use dust.connection
     dust.client)

(define method
  (car (command-line-arguments)))

(define conn
  (connect "~/.dust/dust.sock"))

(pp (command conn method))
(close-connection conn)
