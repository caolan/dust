(use test dust.alist-match)

(test-group "simple key matching"
  (test '(1 2)
        (alist-match '((a . 1) (b . 2) (c . 3))
                     ((a b) (list a b)))))

(test-group "multiple clauses"
  (test 123
        (alist-match '((a . 1) (b . 2) (c . 3) (d . 4))
          ((a x) 'nope)
          ((b e f) 'nope)
          ((d) 123))))

(test-group "match value"
  (test "Example"
        (alist-match '((type . "post") (title . "Example"))
          (((type . "comment") author text)
           #f)
          (((type . "post") title)
           title))))

(test-group "no match"
  (test 'no-match
        (condition-case
            (alist-match '((a . 1))
              ((b) 'match))
          ((exn match) 'no-match))))

(test-exit)
        
