(use test posix files srfi-1 srfi-13)

(define test-dir "tests")

(define (test-file? filename)
  (and (string-prefix? "test-" filename)
       (string=? "scm" (pathname-extension filename))))
        
(define (run-test-file filename)
  (print filename)
  (print (make-string (string-length filename) #\=))
  (load (make-pathname test-dir filename)))

;; disable test-exit until all tests have run
(fluid-let ((test-exit void))
  (for-each run-test-file
            (filter test-file? (directory test-dir))))

(test-exit)
