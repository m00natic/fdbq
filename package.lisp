;;;; package.lisp

(defpackage #:fdbq
  (:use #:cl)
  (:export #:defspec
           #:select
           #:select*
           #:cnt
           #:cnt*
           #:gen-where
           #:gen-do-lines
           #:gen-select
           #:gen-print-selection
           #:gen-list-selection
           #:gen-print-select-results
           #:append-vec
           #:gen-cnt))
