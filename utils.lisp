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
     ,(if (and print (= 1 jobs))
          (gen-do-lines spec 'line
                        ;; if where is empty, condition is considered always satisfied
                        `((when ,(or (gen-where where 'line spec) t)
                            ,(gen-print-selection spec field-list 'line))))
          `(let ((res ,(gen-do-lines spec 'line
                                     `((when ,(or (gen-where where 'line spec) t)
                                         ,(gen-list-selection spec field-list 'line 'result)))
                                     :reduce-fn 'nconc :jobs jobs
                                     :result-var 'result :result-initform nil
                                     :result-type 'list)))
             ,(if print
                  (gen-print-select-results 'res (length field-list))
                  'res)))))

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
  `(dolist (line ,res-var)
     ,(let* ((fmt-size (* 3 field-count))
             (fmt-str (make-string (+ fmt-size 3) :element-type 'base-char)))
        (loop for i from 0 below fmt-size by 3
              do (setf (aref fmt-str i) #\|
                       (aref fmt-str (1+ i)) #\~
                       (aref fmt-str (+ 2 i)) #\A))
        (setf (aref fmt-str fmt-size) #\|
              (aref fmt-str (1+ fmt-size)) #\~
              (aref fmt-str (+ 2 fmt-size)) #\%)
        `(format t ,fmt-str ,@(loop for i from 0 below field-count
                                    collect `(aref line ,i))))))

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
             for i from 0
             collect `(setf (aref res ,i) (subseq ,line-var ,(field-offset field spec)
                                                  :end ,(+ (field-offset field spec)
                                                           (field-size field spec)))))
     (setf ,result (nconc ,result (list res)))))

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
