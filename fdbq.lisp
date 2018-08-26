;;;; fdbq.lisp

(in-package #:fdbq)

(defconstant +max-buffer-size+ (* 1024 6)
  "Max chunk size to preload in bytes.")

(defmacro select (field-list db &key where)
  "Generate selection procedure and run it."
  (let ((spec (get-spec db)))  ;pull out the specification for this db
    `(funcall (compile nil (lambda ()  ;guarantee execution of a compiled object
                             (declare (optimize (speed 3) (debug 0) (safety 0)
                                                (compilation-speed 0)))
                             (do-lines (line ,spec) ;bind line to db entries
                               ;; if where is empty, condition is considered always satisfied
                               (when ,(or (gen-where where 'line spec) t)
                                 ,(gen-print-selection field-list 'line spec))))))))

(defmacro do-lines ((line-var spec) &body body)
  "Bind LINE-VAR to each line in SPEC specified file and execute BODY."
  (let* ((entry-size (spec-size spec))  ;entry size is known
         (line-size (1+ entry-size))    ;add 1 for newline
         (buffer-size (* line-size (floor +max-buffer-size+ line-size)))
         (ins (gensym)) ;make sure the file stream variable is not visible to the body
         (buffer (gensym))    ;-//- for read buffer
         (bytes (gensym))     ;-//- for number of bytes read
         (offset (gensym)))   ;-//- current line within buffer offset)
    `(let ((,buffer (make-array ,buffer-size :element-type 'ascii:ub-char)) ;allocate read
           (,line-var (make-string ,entry-size :element-type 'base-char))) ;and line buffers
       (declare (type (simple-array ascii:ub-char (,buffer-size)) ,buffer)
                (type (simple-base-string ,entry-size) ,line-var) ;no need for newline
                (dynamic-extent ,buffer ,line-var)) ;use stack allocation if possible
       (with-open-file (,ins ,(spec-path spec) :direction :input
                                               :element-type 'ascii:ub-char)
         (loop for ,bytes fixnum = (read-sequence ,buffer ,ins) ;read as many lines
               until (zerop ,bytes)
               ;; slide offset through the buffer
               do (loop for ,offset fixnum from 0 below (* ,line-size
                                                           (floor ,bytes ,line-size))
                        by ,line-size
                        ;; fill line with the current window bytes while converting them
                        do (loop for i fixnum from 0 below ,entry-size
                                 for j fixnum from ,offset
                                 do (setf (aref ,line-var i) (code-char (aref ,buffer j))))
                        ,@body)))))) ;the body only sees the line variable as before

(defun gen-where (where line-var spec)
  "Create actual boolean tree for WHERE.
  LINE-VAR is symbol representing the current line variable.
  SPEC contains fields' offset and size information."
  (when (consp where)
    (let ((op (first where)))
      (cond ((consp op)         ;several expressions in a row, recurse
             (cons (gen-where op line-var spec)
                   (gen-where (rest where) line-var spec)))
            ((member op '(and or not))  ;intermediate node, recurse
             (cons op (gen-where (rest where) line-var spec)))
            ((member op '(= /= < <= > >= like)) ;leaf
             (gen-field-op where line-var spec))
            (t (error (format nil "Bad where clause: ~A" where)))))))

(defun translate-op (op &optional char?)
  "Return string/char operation corresponding to OP."
  (intern (concatenate 'string (if char? "CHAR" "STRING")
                       (symbol-name op))))

(defun simple-regex? (str)
  "Check if string is not really a regex.
This a bit pessimistic."
  (not (find-if-not #'alphanumericp str)))

(defmethod gen-field-clause ((op (eql 'like)) field1 field2 line-var)
  "Generate code for regex clause."
  (cond ((null (field-filter field2))   ;regex is taken from a field
         `(cl-ppcre:scan (subseq ,line-var ,(db-field-offset field2)
                                 :end ,(+ (db-field-offset field2)
                                          (db-field-size field2)))
                         ,line-var :start ,(db-field-offset field1)
                                   :end ,(+ (db-field-offset field1)
                                            (db-field-size field1))))
        ((simple-regex? (field-filter field2)) ;use plain search instead of regex
         `(search ,(field-filter field2) ,line-var
                  :start1 ,(db-field-offset field2)
                  :end1 ,(+ (db-field-offset field2)
                            (db-field-size field2))
                  :start2 ,(db-field-offset field1)
                  :end2 ,(+ (db-field-offset field1)
                            (db-field-size field1))))
        (t `(cl-ppcre:scan ,(field-filter field2)
                           ,line-var :start ,(db-field-offset field1)
                                     :end ,(+ (db-field-offset field1)
                                              (db-field-size field1))))))

(defmethod gen-field-clause (op field1 field2 line-var)
  "Generate code for a comparison clause."
  (let ((size (min (db-field-size field1)
                   (db-field-size field2))))
    (if (= 1 size)               ;optimize single character comparison
        (list (translate-op op t)
              (if (field-filter field1) ;string literal?
                  (aref (field-filter field1) 0)
                  `(aref ,line-var ,(db-field-offset field1)))
              (if (field-filter field2)
                  (aref (field-filter field2) 0)
                  `(aref ,line-var ,(db-field-offset field2))))
        (list (translate-op op)
              (or (field-filter field1) line-var)
              (or (field-filter field2) line-var)
              :start1 (db-field-offset field1)
              :end1 (+ (db-field-offset field1) size)
              :start2 (db-field-offset field2)
              :end2 (+ (db-field-offset field2) size)))))

(defun gen-field-op (clause line-var spec)
  "Generate code for a leaf WHERE clause."
  (destructuring-bind (op field1 field2) clause
    (gen-field-clause op (get-operand-traits field1 spec)
                      (get-operand-traits field2 spec) line-var)))

(defun gen-print-selection (fields line-var spec)
  "Unroll selected FIELDS' print statements.
  LINE-VAR is symbol representing the current line variable.
  SPEC holds field offset details."
  `(progn
     ,@(loop for field in fields ;collect print statements in list and splice them
             collect `(write-string ,line-var nil
                                    :start ,(field-offset field spec)
                                    :end ,(+ (field-offset field spec) ;constant fold
                                             (field-size field spec))))
     (format t "~%")))
