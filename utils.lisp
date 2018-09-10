;;;; utils.lisp

(in-package #:fdbq)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (defvar *optimize* '(optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0))))

(defgeneric gen-do-lines (spec line-var body
                          &key result-var result-type result-initform
                            jobs reduce-fn &allow-other-keys)
  (:documentation "Generator of generic entry iteration code over BODY
with LINE-VAR bound to current line string.
RESULT-VAR with optionally specified RESULT-TYPE and initial value
RESULT-INITFORM can be used to gather information over the whole iteration.
Number of parallel JOBS may be specified.
REDUCE-FN can be used to reduce results over 2 jobs."))

(defgeneric gen-select (spec field-list where print jobs)
  (:documentation "Generate selection procedure for FIELD-LIST from SPEC db with WHERE filter.
If PRINT is nil, return list of results otherwise pretty print selection."))

(defmethod gen-select (spec field-list where print jobs)
  "Generate selection procedure for FIELD-LIST from DB with WHERE filter.
Default implementation using only string line."
  `(lambda () (declare ,*optimize*)
     ,(cond
        ((and print (= 1 jobs))
         (gen-do-lines spec 'line
                       ;; if where is empty, condition is considered always satisfied
                       `((when ,(or (gen-where where 'line spec) t)
                           ,(gen-print-selection field-list 'line)))))
        (print
         (alexandria:with-gensyms (reduce-print)
           `(flet ((,reduce-print (vec1 vec2)
                     (declare ,*optimize*
                              (type (vector (simple-array simple-base-string)) vec1 vec2))
                     ,(gen-print-select-results 'vec2 (length field-list))
                     vec1))
              (declare (inline ,reduce-print))
              ,(gen-do-lines spec 'line
                             `((when ,(or (gen-where where 'line spec) t)
                                 ,(gen-list-selection field-list 'line 'result)))
                             :reduce-fn reduce-print :jobs jobs
                             :result-var 'result
                             :result-initform '(make-array 0 :fill-pointer t :adjustable t)
                             :result-type `(vector (simple-array simple-base-string
                                                                 (,(length field-list))))))))
        (t (gen-do-lines spec 'line
                         `((when ,(or (gen-where where 'line spec) t)
                             ,(gen-list-selection field-list 'line 'result)))
                         :reduce-fn 'append-vec :jobs jobs
                         :result-var 'result
                         :result-initform '(make-array 0 :fill-pointer t :adjustable t)
                         :result-type `(vector (simple-array simple-base-string
                                                             (,(length field-list)))))))))

(defun get-select-fields (spec fields)
  "Return array with field information for selected FIELDS.
If FIELDS is empty, return all SPEC fields."
  (if fields
      (let ((selection (make-array (length fields)))
            (spec-fields (spec-fields spec)))
        (loop for i fixnum from 0 below (length selection)
              for field in fields
              do (setf (aref selection i) (gethash field spec-fields)))
        selection)
      (field-list spec)))

(defun gen-print-selection (fields line-var &optional offset-var)
  "Unroll selected FIELDS' print statements.
LINE-VAR is symbol representing the current string line variable.
If OFFSET-VAR is non-nil, then it's a symbol representing the current offset within buffer,
LINE-VAR in this case is treated as the byte buffer."
  `(progn
     ,@(loop for field across fields
             collect '(write-char #\|)
             collect
             (if offset-var
                 `(loop for i fixnum from (+ ,offset-var
                                             ,(field-offset field))
                          below (+ ,offset-var ,(+ (field-offset field)
                                                   (field-size field)))
                        do (write-char (code-char (aref ,line-var i))))
                 `(write-string ,line-var nil
                                :start ,(field-offset field)
                                :end ,(+ (field-offset field)
                                         (field-size field)))))
     (format t "|~%")))

(defun gen-list-selection (fields line-var result &optional offset-var)
  "Unroll selected FIELDS' gather-in RESULT statements.
LINE-VAR is symbol representing the current line variable.
If OFFSET-VAR is non-nil, then it's a symbol representing the current offset within buffer,
LINE-VAR in this case is treated as the byte buffer."
  `(let ((res (make-array ,(length fields)
                          :element-type 'simple-base-string
                          :initial-contents
                          (list
                           ,@(loop for field across fields
                                   collect (if offset-var
                                               `(let ((field-str (make-string ,(field-size field)
                                                                              :element-type 'base-char)))
                                                  (loop for i fixnum from 0 below ,(field-size field)
                                                        for j fixnum from (+ ,offset-var ,(field-offset field))
                                                        do (setf (aref field-str i) (code-char (aref ,line-var j))))
                                                  field-str)
                                               `(subseq ,line-var ,(field-offset field)
                                                        :end ,(+ (field-offset field)
                                                                 (field-size field)))))))))
     (vector-push-extend res ,result)))

(defun gen-print-select-results (res-var field-count)
  "Pretty print list of results."
  `(loop for i fixnum from 0 below (length ,res-var)
         do (let ((line (aref ,res-var i)))
              (declare (type (simple-array simple-base-string (,field-count)) line))
              ,(let ((fmt-str (with-output-to-string (s nil :element-type 'base-char)
                                (loop repeat field-count
                                      do (write-string "|~A" s))
                                (write-string "|~%" s))))
                 `(format t ,fmt-str ,@(loop for i fixnum from 0 below field-count
                                             collect `(aref line ,i)))))))

(defgeneric gen-cnt (spec where jobs)
  (:documentation "Generate count procedure for SPEC db with WHERE filter."))

(defmethod gen-cnt (spec where jobs)
  "Generate count procedure over DB with WHERE filter using string line."
  `(lambda () (declare ,*optimize*)
     ,(gen-do-lines spec 'line
                    ;; if where is empty, condition is considered always satisfied
                    `((when ,(or (gen-where where 'line spec) t)
                        (incf result)))
                    :reduce-fn '+ :jobs jobs
                    :result-var 'result :result-initform 0 :result-type 'fixnum)))

(declaim (inline append-vec))

(defun append-vec (vec1 vec2)
  "Append VEC2 to the end of VEC1."
  (declare #.*optimize*
           (type (vector (simple-array simple-base-string)) vec1 vec2))
  (loop for el across vec2
        do (vector-push-extend el vec1))
  vec1)

(defun run-compiled (proc)
  "Compile and then run anonymous PROC."
  (funcall (compile nil proc)))
