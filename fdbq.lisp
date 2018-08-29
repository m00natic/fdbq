;;;; fdbq.lisp

(in-package #:fdbq)

(defun gen-select (field-list db where)
  "Generate selection procedure for FIELD-LIST from DB with WHERE filter."
  (let ((spec (get-spec db)))  ;pull out the specification for this db
    `(lambda () (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
       ,(gen-do-lines spec 'line
          ;; if where is empty, condition is considered always satisfied
          `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
              ,(gen-print-selection field-list 'line spec
                                    'buffer 'offset)))
          :buffer-var 'buffer :offset-var 'offset))))

(defun gen-print-selection (fields line-var spec
                            &optional buffer-var offset-var)
  "Unroll selected FIELDS' print statements.
LINE-VAR is symbol representing the current line variable.
SPEC holds field offset details.
BUFFER-VAR is symbol representing the db buffer.
OFFSET-VAR is symbol representing the current offset in the db buffer."
  `(progn
     ,@(if buffer-var
           (loop for field in fields ;collect print statements in list and splice them
                 collect '(write-char #\|)
                 collect `(loop for i fixnum from (+ ,offset-var
                                                     ,(field-offset field spec))
                                  below (+ ,offset-var ,(+ (field-offset field spec)
                                                           (field-size field spec)))
                                do (write-char (code-char (aref ,buffer-var i)))))
           (loop for field in fields ;collect print statements in list and splice them
                 collect '(write-char #\|)
                 collect `(write-string ,line-var nil
                                        :start ,(field-offset field spec)
                                        :end ,(+ (field-offset field spec) ;constant fold
                                                 (field-size field spec)))))
     (format t "|~%")))

(defun select* (field-list db &key where)
  "Select FIELD-LIST from DB with WHERE filter."
  (funcall (compile nil (gen-select field-list db where))))

(defmacro select (field-list db &key where)
  "Select FIELD-LIST from DB with WHERE filter."
  `(funcall (compile nil ,(gen-select field-list db where))))
