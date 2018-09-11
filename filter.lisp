;;;; filter.lisp

(in-package #:fdbq)

(defun gen-where (where line-var spec &optional buffer-var offset-var)
  "Create actual boolean tree for WHERE.
LINE-VAR is symbol representing the current line variable.
SPEC contains fields' offset and size information.
BUFFER-VAR is symbol representing the db buffer.
OFFSET-VAR is symbol representing the current offset in the db buffer.
If buffer and offset are nil, use operations only over the string line.
LINE-VAR, BUFFER-VAR and OFFSET-VAR are dynamic."
  (declare (special line-var buffer-var offset-var))
  (when (consp where)
    (let ((op (first where)))
      (cond ((consp op)         ;several expressions in a row, recurse
             (cons (gen-where op line-var spec buffer-var offset-var)
                   (gen-where (rest where) line-var spec buffer-var offset-var)))
            ((member op '(and or not))  ;intermediate node, recurse
             (cons op (gen-where (rest where) line-var spec buffer-var offset-var)))
            ((member op '(= /= < <= > >= like)) ;leaf
             (gen-field-op where spec))
            (t (error (format nil "Bad where clause: ~A" where)))))))

(defun gen-field-op (clause spec)
  "Generate code for a leaf WHERE clause."
  (declare (special buffer-var))
  (destructuring-bind (op field1 field2) clause
    (if buffer-var
        (gen-field-clause-raw op (get-operand-traits field1 spec)
                              (get-operand-traits field2 spec))
        (gen-field-clause op (get-operand-traits field1 spec)
                          (get-operand-traits field2 spec)))))

(defun translate-op (op &optional char?)
  (declare (special buffer-var))
  "Return string/char operation corresponding to OP."
  (if buffer-var
      (intern (concatenate 'string
                           (if char? "UB-CHAR" "UB-STRING")
                           (symbol-name op)) :ascii)
      (intern (concatenate 'string
                           (if char? "CHAR" "STRING")
                           (symbol-name op)))))

(defun simple-regex? (str)
  "Check if string is not really a regex.
This a bit pessimistic."
  (not (find-if-not #'alphanumericp str)))

(defgeneric gen-field-clause (op field1 field2)
  (:documentation "Generate code for operation with 2 operands over string line."))

(defgeneric gen-field-clause-raw (op field1 field2)
  (:documentation "Generate code for operation with 2 operands over byte buffer with offset."))

(defmethod gen-field-clause ((op (eql 'like)) field1 field2)
  "Generate code for regex clause over string line variable."
  (declare (special line-var))
  (with-slots ((offset1 offset) (size1 size)) field1
    (with-slots ((offset2 offset) (size2 size) (filter2 filter)) field2
      (cond ((null filter2)             ;regex is taken from a field
             `(cl-ppcre:scan (subseq ,line-var ,offset2
                                     :end ,(+ offset2 size2))
                             ,line-var :start ,offset1
                                       :end ,(+ offset1 size1)))
            ((simple-regex? filter2) ;use plain search instead of regex
             `(search ,filter2 ,line-var
                      :start1 ,offset2 :end1 ,(+ offset2 size2)
                      :start2 ,offset1 :end2 ,(+ offset1 size1)))
            (t `(cl-ppcre:scan ,filter2
                               ,line-var :start ,offset1
                                         :end ,(+ offset1 size1)))))))

(defmethod gen-field-clause (op field1 field2)
  "Generate code for a comparison clause over string line variable."
  (declare (special line-var))
  (with-slots ((offset1 offset) (size1 size) (filter1 filter)) field1
    (with-slots ((offset2 offset) (size2 size) (filter2 filter)) field2
      (let ((size (min size1 size2)))
        (if (= 1 size)           ;optimize single character comparison
            (list (translate-op op t)
                  (if filter1           ;string literal?
                      (aref filter1 0)
                      `(aref ,line-var ,offset1))
                  (if filter2
                      (aref filter2 0)
                      `(aref ,line-var ,offset2)))
            (list (translate-op op)
                  (or filter1 line-var) (or filter2 line-var)
                  :start1 offset1 :end1 (+ offset1 size)
                  :start2 offset2 :end2 (+ offset2 size)))))))

(defmethod gen-field-clause-raw ((op (eql 'like)) field1 field2)
  "Generate code for regex clause over raw byte buffer."
  (declare (special line-var buffer-var offset-var))
  (with-slots ((offset1 offset) (size1 size)) field1
    (with-slots ((offset2 offset) (size2 size) (filter2 filter)) field2
      (cond ((null filter2) ;regex is taken from a field
             `(progn (loop for i fixnum from ,offset1
                             below ,(+ offset1 size1)
                           for j fixnum from (+ ,offset-var ,offset1)
                           do (setf (aref ,line-var i)
                                    (code-char (aref ,buffer-var j))))
                     (loop for i fixnum from ,offset2
                             below ,(+ offset2 size2)
                           for j fixnum from (+ ,offset-var ,offset2)
                           do (setf (aref ,line-var i)
                                    (code-char (aref ,buffer-var j))))
                     (cl-ppcre:scan (subseq ,line-var ,offset2
                                            :end ,(+ offset2 size2))
                                    ,line-var :start ,offset1
                                    :end ,(+ offset1 size1))))
            ((simple-regex? filter2) ;use plain search instead of regex
             `(search ,(ascii:string-to-ub filter2) ,buffer-var
                      :start1 ,offset2 :end1 ,(+ offset2 size2)
                      :start2 (+ ,offset-var ,offset1)
                      :end2 (+ ,offset-var ,(+ offset1 size1))))
            (t `(progn (loop for i fixnum from ,offset1
                               below ,(+ offset1 size1)
                             for j fixnum from (+ ,offset-var ,offset1)
                             do (setf (aref ,line-var i)
                                      (code-char (aref ,buffer-var j))))
                       (cl-ppcre:scan ,filter2 ,line-var
                                      :start ,offset1
                                      :end ,(+ offset1 size1))))))))

(defmethod gen-field-clause-raw (op field1 field2)
  "Generate code for a comparison clause over raw byte buffer."
  (declare (special buffer-var offset-var))
  (with-slots ((offset1 offset) (size1 size) (filter1 filter)) field1
    (with-slots ((offset2 offset) (size2 size) (filter2 filter)) field2
      (let ((size (min size1 size2)))
        (flet ((gen-field1-char (i)
                 (if filter1              ;string literal?
                     (char-code (aref filter1 i))
                     `(aref ,buffer-var (+ ,offset-var ,(+ offset1 i)))))
               (gen-field2-char (i)
                 (if filter2
                     (char-code (aref filter2 i))
                     `(aref ,buffer-var (+ ,offset-var ,(+ offset2 i))))))
          (cond ((= 1 size)           ;optimize single character comparison
                 (list op (gen-field1-char 0) (gen-field2-char 0)))
                ((member op '(= /=))
                 `(and ,@(loop for i from 0 below size ;unroll exact comparison
                               collect (list op (gen-field1-char i)
                                             (gen-field2-char i)))))
                (t (let ((last-i (1- size)))
                     `(cond
                        ,@(loop for i from 0 below last-i
                                collect (let ((op1 (gen-field1-char i))
                                              (op2 (gen-field2-char i)))
                                          `((/= ,op1 ,op2)
                                            ,(list op op1 op2))))
                        (t ,(list op (gen-field1-char last-i)
                                  (gen-field2-char last-i))))))))))))
