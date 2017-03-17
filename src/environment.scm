(module dust.environment *

(import chicken scheme)
  
(use posix
     files
     pathname-expand
     data-structures)

(define dust-path
  (make-parameter
   (pathname-expand
    (or (alist-ref "DUST_PATH" (get-environment-variables) string=?)
        "~/.dust"))))

(define private-keys-path
  (make-parameter
   (make-pathname (dust-path) "private-keys")))

(define streams-path
  (make-parameter
   (make-pathname (dust-path) "streams")))

(define socket-path
  (make-parameter
   (make-pathname (dust-path) "dust.sock")))

)
