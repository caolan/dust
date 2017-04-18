(module dust.alist-utils

;;;;; Exports ;;;;;
(match-alist
 match-hash-table
 match-hash-table-patterns)

(import chicken scheme)
(use srfi-1 srfi-69)


(define (match-hash-table-patterns ht keys)
  (call/cc
   (lambda (k)
     (condition-case
         (map (lambda (pattern)
                (if (pair? pattern)
                    (let ((val (hash-table-ref ht (car pattern))))
                      (if (equal? val (cdr pattern))
                          val
                          (k #f)))
                    (hash-table-ref ht pattern)))
              keys)
       ((exn access) #f)))))

(define-syntax format-patterns
  (er-macro-transformer
   (lambda (exp rename compare)
     (cons (rename 'list)
           (map (lambda (p)
                  (if (pair? p)
                      (list (rename 'cons)
                            (list (rename 'quote) (car p))
                            (list (rename 'quasiquote) (cdr p)))
                      (list (rename 'quote) p)))
                (cdr exp))))))

(define-syntax format-lambda
  (er-macro-transformer
   (lambda (exp rename compare)
     (append (list (rename 'lambda)
                   (map (lambda (p)
                          (if (pair? p)
                              (car p)
                              p))
                        (cadr exp)))
             (cddr exp)))))

(define-syntax match-hash-table
  (syntax-rules (else)
    ((_ ht (else alternative ...))
     (begin alternative ...))
    ((_ ht ((patterns ...) body ...))
     (cond ((match-hash-table-patterns ht (format-patterns patterns ...)) =>
            (cut apply (format-lambda (patterns ...) body ...) <>))
           (else
            (abort (make-composite-condition
                    (make-property-condition
                     'exn
                     'message "No matching clause")
                    (make-property-condition 'match))))))
    ((_ ht ((patterns ...) body ...) clause ...)
      (cond ((match-hash-table-patterns ht (format-patterns patterns ...)) =>
             (cut apply (format-lambda (patterns ...) body ...) <>))
            (else
             (match-hash-table ht clause ...))))))

(define-syntax match-alist
  (syntax-rules (else)
    ((_ alist clause ...)
     (let ((ht (alist->hash-table alist)))
       (match-hash-table ht clause ...)))))

)
