(module dust.alist-match

;;;;; Exports ;;;;;
(alist-match)

(import chicken scheme)
(use srfi-1)

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

(define-syntax alist-match-pattern
  (syntax-rules ()
    ((_ alist (key . val) escape)
     (let ((m (assq (quote key) alist)))
       (if (and m (equal? (cdr m) (quasiquote val)))
           (cdr m)
           (escape #f))))
    ((_ alist key escape)
     (let ((m (assq (quote key) alist)))
       (if m
           (cdr m)
           (escape #f))))))

(define-syntax alist-match-patterns
  (syntax-rules ()
    ((_ alist key ...)
     (call/cc
      (lambda (escape)
        (list (alist-match-pattern alist key escape) ...))))))

(define-syntax alist-match
  (syntax-rules (else)
    ((_ alist (else alternative ...))
     (begin alternative ...))
    ((_ alist ((pattern ...) body ...) clause ...)
     (let* ((lst alist)
            (match (alist-match-patterns lst pattern ...)))
       (if match
           (apply (format-lambda (pattern ...) body ...) match)
           (alist-match lst clause ...))))
    ((_ alist)
     (abort (make-composite-condition
             (make-property-condition
              'exn
              'message "No matching clause")
             (make-property-condition 'match))))))

)
