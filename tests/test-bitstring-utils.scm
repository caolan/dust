(use test bitstring dust.bitstring-utils)

(test-group "bitstring-split"
  (let ((b (list->bitstring '(1 1 0 1 0 0 1 0))))
    (receive (before after) (bitstring-split b 4)
      (test-assert
          (bitstring=? (list->bitstring '(1 1 0 1)) (identity before)))
      (test-assert
          (bitstring=? (list->bitstring '(0 0 1 0)) (identity after))))
    (receive (before after) (bitstring-split b 3)
      (test-assert
          (bitstring=? (list->bitstring '(1 1 0)) (identity before)))
      (test-assert
          (bitstring=? (list->bitstring '(1 0 0 1 0)) (identity after))))
    (receive (before after) (bitstring-split b 5)
      (test-assert
          (bitstring=? (list->bitstring '(1 1 0 1 0)) (identity before)))
      (test-assert
          (bitstring=? (list->bitstring '(0 1 0)) (identity after))))))

(test-group "bitstring-take"
  (let ((b (list->bitstring '(1 1 0 1 0 0 1 0))))
    (test-assert
        (bitstring=? (list->bitstring '(1 1 0 1))
                     (bitstring-take b 4)))
    (test-assert
        (bitstring=? (list->bitstring '(1 1 0))
                     (bitstring-take b 3)))
    (test-assert
        (bitstring=? (list->bitstring '(1 1 0 1 0))
                     (bitstring-take b 5)))))

(test-group "bitstring-drop"
  (let ((b (list->bitstring '(1 1 0 1 0 0 1 0))))
    (test-assert
        (bitstring=? (list->bitstring '(0 0 1 0))
                     (bitstring-drop b 4)))
    (test-assert
        (bitstring=? (list->bitstring '(1 0 0 1 0))
                     (bitstring-drop b 3)))
    (test-assert
        (bitstring=? (list->bitstring '(0 1 0))
                     (bitstring-drop b 5)))))

(test-group "bitstring-compare"
  (test 0 (bitstring-compare (list->bitstring '(1 0 1))
                             (list->bitstring '(1 0 1))))
  (test 0 (bitstring-compare (list->bitstring '())
                             (list->bitstring '())))
  (test 1 (bitstring-compare (list->bitstring '(1 1 0))
                             (list->bitstring '())))
  (test 1 (bitstring-compare (list->bitstring '(1 1 0))
                             (list->bitstring '(1 1))))
  (test 1 (bitstring-compare (list->bitstring '(1 1 0))
                             (list->bitstring '(1 0 1))))
  (test 1 (bitstring-compare (list->bitstring '(1 1 0))
                             (list->bitstring '(0 1 0 1))))
  (test -1 (bitstring-compare (list->bitstring '())
                              (list->bitstring '(1 1 0))))
  (test -1 (bitstring-compare (list->bitstring '(1 1))
                              (list->bitstring '(1 1 0))))
  (test -1 (bitstring-compare (list->bitstring '(1 0 1))
                              (list->bitstring '(1 1 0))))
  (test -1 (bitstring-compare (list->bitstring '(0 1 0 1))
                              (list->bitstring '(1 1 0)))))

(test-exit)
