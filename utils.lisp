;;;; utils.lisp

(in-package #:fdbq)

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
  `(lambda () (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
     ,(cond
        ((and print (= 1 jobs))
         (gen-do-lines spec 'line
                       ;; if where is empty, condition is considered always satisfied
                       `((when ,(or (gen-where where 'line spec) t)
                           ,(gen-print-selection spec field-list 'line)))))
        (print
         (alexandria:with-gensyms (reduce-print)
           `(flet ((,reduce-print (vec1 vec2)
                     (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0))
                              (type (vector (simple-array simple-base-string)) vec1 vec2))
                     ,(gen-print-select-results 'vec2 (length field-list))
                     vec1))
              (declare (inline ,reduce-print))
              ,(gen-do-lines spec 'line
                             `((when ,(or (gen-where where 'line spec) t)
                                 ,(gen-list-selection spec field-list 'line 'result)))
                             :reduce-fn reduce-print :jobs jobs
                             :result-var 'result
                             :result-initform '(make-array 0 :fill-pointer t :adjustable t)
                             :result-type `(vector (simple-array simple-base-string
                                                                 (,(length field-list))))))))
        (t (gen-do-lines spec 'line
                         `((when ,(or (gen-where where 'line spec) t)
                             ,(gen-list-selection spec field-list 'line 'result)))
                         :reduce-fn 'append-vec :jobs jobs
                         :result-var 'result
                         :result-initform '(make-array 0 :fill-pointer t :adjustable t)
                         :result-type `(vector (simple-array simple-base-string
                                                             (,(length field-list)))))))))

(defgeneric gen-print-selection (spec fields line-var &key &allow-other-keys)
  (:documentation "Generate printing of FIELDS selection over SPEC db code.
LINE-VAR is symbol representing the current line variable.
SPEC holds field offset details."))

(defmethod gen-print-selection (spec fields line-var &key &allow-other-keys)
  "Unroll selected FIELDS' print statements.
LINE-VAR is symbol representing the current line variable.
SPEC holds field offset details."
  `(progn
     ,@(loop for field in fields ;collect print statements in list and splice them
             collect '(write-char #\|)
             collect `(write-string ,line-var nil
                                    :start ,(field-offset field spec)
                                    :end ,(+ (field-offset field spec)
                                             (field-size field spec))))
     (format t "|~%")))

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

(defgeneric gen-list-selection (spec fields line-var result &key &allow-other-keys)
  (:documentation "Generate FIELDS selection to list over SPEC db code.
LINE-VAR is symbol representing the current line variable.
SPEC holds field offset details."))

(defmethod gen-list-selection (spec fields line-var result &key &allow-other-keys)
  "Unroll selected FIELDS' gather statements.
LINE-VAR is symbol representing the current line variable.
SPEC holds field offset details."
  `(let ((res (make-array ,(length fields) :element-type 'simple-base-string)))
     ,@(loop for field in fields
             for i fixnum from 0
             collect `(setf (aref res ,i) (subseq ,line-var ,(field-offset field spec)
                                                  :end ,(+ (field-offset field spec)
                                                           (field-size field spec)))))
     (vector-push-extend res ,result)))

(defgeneric gen-cnt (spec where jobs)
  (:documentation "Generate count procedure for SPEC db with WHERE filter."))

(defmethod gen-cnt (spec where jobs)
  "Generate count procedure over DB with WHERE filter using string line."
  `(lambda () (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
     ,(gen-do-lines spec 'line
                    ;; if where is empty, condition is considered always satisfied
                    `((when ,(or (gen-where where 'line spec) t)
                        (incf result)))
                    :reduce-fn '+ :jobs jobs
                    :result-var 'result :result-initform 0 :result-type 'fixnum)))

(proclaim '(inline append-vec))

(defun append-vec (vec1 vec2)
  "Append VEC2 to the end of VEC1."
  (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0))
           (type (vector (simple-array simple-base-string)) vec1 vec2))
  (loop for el across vec2
        do (vector-push-extend el vec1))
  vec1)
