;;;; storage-file.lisp

(in-package #:fdbq)

(defvar *buffer-size* (/ (expt 1024 2) 2)
  "Bytes per job.")

(defclass spec-file (spec)
  ((path :type pathname :accessor spec-path :initarg :path)))

(defmethod defspec (name (type (eql :file)) fields &key path)
  "Register file DB with NAME, list of FIELDS and file PATH."
  (let ((spec (make-instance 'spec-file :path (merge-pathnames path))))
    (defspec-read-fields spec fields)
    (setf (gethash name *dbs*) spec)))

(defmethod gen-select ((spec spec-file) field-list where print jobs)
  "Generate selection procedure for FIELD-LIST from DB with WHERE filter."
  `(lambda () (declare ,*optimize*)
     ,(cond
        ((and print (= 1 jobs))
         (gen-do-lines spec 'line
                       `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                           ,(gen-print-selection field-list 'buffer 'offset)))
                       :buffer-var 'buffer :offset-var 'offset))
        (print
         (alexandria:with-gensyms (reduce-print)
           `(flet ((,reduce-print (vec1 vec2)
                     (declare ,*optimize*
                              (type (vector (simple-array simple-base-string)) vec1 vec2))
                     ,(gen-print-select-results 'vec2 (length field-list))
                     vec1))
              (declare (inline ,reduce-print))
              ,(gen-do-lines spec 'line
                             `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                                 ,(gen-list-selection field-list 'buffer 'result 'offset)))
                             :buffer-var 'buffer :offset-var 'offset
                             :reduce-fn reduce-print :jobs jobs
                             :result-var 'result
                             :result-initform '(make-array 0 :fill-pointer t :adjustable t)
                             :result-type `(vector (simple-array simple-base-string
                                                                 (,(length field-list))))))))
        (t (gen-do-lines spec 'line
                         `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                             ,(gen-list-selection field-list 'buffer 'result 'offset)))
                         :buffer-var 'buffer :offset-var 'offset
                         :reduce-fn (if print 'reduce-print 'append-vec)
                         :jobs jobs :result-var 'result
                         :result-initform '(make-array 0 :fill-pointer t :adjustable t)
                         :result-type `(vector (simple-array simple-base-string
                                                             (,(length field-list)))))))))

(defmethod gen-cnt ((spec spec-file) where jobs)
  "Generate count procedure over DB with WHERE filter over file."
  `(lambda () (declare ,*optimize*)
     ,(gen-do-lines spec 'line
                    `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                        (incf result)))
                    :buffer-var 'buffer :offset-var 'offset
                    :reduce-fn '+ :jobs jobs
                    :result-var 'result :result-initform 0 :result-type 'fixnum)))

#-ccl
(defmethod gen-do-lines ((spec spec-file) line-var body
                         &key (result-var (gensym)) result-type result-initform
                           (jobs 1) reduce-fn
                           buffer-var offset-var)
  "File db iteration.  BUFFER-VAR and OFFSET-VAR get bound
to raw byte buffer and current line offset within it respectively."
  (let* ((line-size (1+ (spec-size spec)))    ;add 1 for newline
         (buffer-size (if (< line-size *buffer-size*)
                          (* line-size (floor *buffer-size* line-size))
                          line-size))
         (threading? (< 1 jobs)))
    (alexandria:with-gensyms (file-lock buffer-array line-array
                                        job-id job-count portion-id ins take-count)
      `(let ((,buffer-array (make-array ,jobs :element-type '(simple-array ascii:ub-char
                                                              (,buffer-size))
                                              :initial-contents
                                              (list ,@(loop repeat jobs
                                                            collect `(make-array ,buffer-size
                                                                                 :element-type 'ascii:ub-char)))))
             (,line-array (make-array ,jobs :element-type '(simple-base-string ,(1- line-size))
                                            :initial-contents
                                            (list ,@(loop repeat jobs
                                                          collect `(make-array ,(1- line-size)
                                                                               :element-type 'base-char)))))
             (,job-count 0)
             (,file-lock ,(when threading?
                            (bt:make-lock "gen-do-lines"))))
         (declare (dynamic-extent ,line-array)
                  ,@(if threading?
                        `((type fixnum ,job-count))
                        `((dynamic-extent ,buffer-array)
                          (ignore ,job-count ,file-lock))))
         (with-open-file (,ins ,(spec-path spec) :direction :input
                                                 :element-type 'ascii:ub-char)
           (flet ((mapper (,job-id)
                    (declare ,*optimize*
                             (type fixnum ,job-id))
                    (let ((,line-var (aref ,line-array ,job-id))
                          (,result-var ,result-initform)
                          (,buffer-var (aref ,buffer-array ,job-id))
                          (,take-count 0)
                          (,portion-id 0))
                      (declare ,(if result-type
                                    `(type ,result-type ,result-var)
                                    `(ignorable ,result-var))
                               (type (simple-base-string ,(1- line-size)) ,line-var)
                               (ignorable ,line-var)
                               (type (simple-array ascii:ub-char (,buffer-size)) ,buffer-var)
                               (type fixnum ,take-count)
                               ,(if threading?
                                    `(type fixnum ,portion-id)
                                    `(ignore ,portion-id)))
                      ,(if threading?
                           `(bt:with-lock-held (,file-lock)
                              (setf ,take-count (read-sequence ,buffer-var ,ins)
                                    ,portion-id (incf ,job-count)))
                           `(setf ,take-count (read-sequence ,buffer-var ,ins)))
                      (loop for ,offset-var fixnum from 0 below ,take-count by ,line-size
                            do ,@body)
                      ,(if threading?
                           `(cons (cons ,portion-id ,result-var)
                                  (when (= ,take-count ,buffer-size)
                                    ,job-id))
                           `(values ,result-var (= ,take-count ,buffer-size))))))
             ,(cond
                (threading?
                 `(let* ((result ,result-initform)
                         (lparallel:*kernel* (lparallel:make-kernel ,jobs))
                         (chan (lparallel:make-channel)))
                    ,(when result-type
                       `(declare (type ,result-type result)))
                    (unwind-protect
                         (progn
                           (loop for i fixnum from 0 below ,jobs
                                 do (lparallel:submit-task chan #'mapper i))
                           (let ((ready-res (make-array 0 :fill-pointer 0 :adjustable t))
                                 (next 1))
                             (declare (type (vector (cons fixnum ,(or result-type t))) ready-res)
                                      (type fixnum next)
                                      (dynamic-extent ready-res))
                             (loop with more? = t
                                   while more?
                                   do (destructuring-bind (res . job-id)
                                          (lparallel:receive-result chan)
                                        (if job-id
                                            (lparallel:submit-task chan #'mapper job-id)
                                            (setf more? nil))
                                        (vector-push-extend res ready-res))
                                   #1=(loop for res = (find next ready-res :key #'car)
                                        while res
                                        do (locally
                                               (declare (type (cons fixnum ,(or result-type t))
                                                              res))
                                             (setf result (,reduce-fn result (cdr res))
                                                   ready-res (delete next ready-res
                                                                     :key #'car :test #'=)))
                                           (incf next)))
                             (lparallel:do-fast-receives (res chan (1- ,jobs))
                               (vector-push-extend (car res) ready-res)
                               #1#)))
                      (lparallel:end-kernel))
                    result))
                (reduce-fn
                 `(let ((result ,result-initform))
                    ,(when result-type
                       `(declare (type ,result-type result)))
                    (loop with more? = t
                          while more?
                          do (multiple-value-bind (res more) (mapper 0)
                               (setf result (,reduce-fn result res)
                                     more? more)))
                    result))
                (t `(loop with more? = t
                          while more?
                          do (multiple-value-bind (res more) (mapper 0)
                               (declare (ignore res))
                               (setf more? more)))))))))))

#+ccl
(defmethod gen-do-lines ((spec spec-file) line-var body
                         &key (result-var (gensym)) result-type result-initform
                           (jobs 1) reduce-fn
                           buffer-var offset-var)
  "File db iteration.  BUFFER-VAR and OFFSET-VAR get bound
to raw byte buffer and current line offset within it respectively."
  (let* ((line-size (1+ (spec-size spec)))    ;add 1 for newline
         (buffer-size (if (< line-size *buffer-size*)
                          (* line-size (floor *buffer-size* line-size))
                          line-size))
         (threading? (< 1 jobs)))
    (alexandria:with-gensyms (buffer-array line-array take-end job-id portion-id)
      `(let ((,buffer-array (make-array ,jobs :element-type '(simple-array ascii:ub-char
                                                              (,buffer-size))
                                              :initial-contents
                                              (list ,@(loop repeat jobs
                                                            collect `(make-array ,buffer-size
                                                                                 :element-type 'ascii:ub-char)))))
             (,line-array (make-array ,jobs :element-type '(simple-base-string ,(1- line-size))
                                            :initial-contents
                                            (list ,@(loop repeat jobs
                                                          collect `(make-array ,(1- line-size)
                                                                               :element-type 'base-char))))))
         (declare ,@(if threading?
                        `((dynamic-extent ,line-array))
                        `((dynamic-extent ,buffer-array ,line-array))))
         (flet ((mapper (,take-end ,job-id ,portion-id)
                  (declare ,*optimize*
                           (type fixnum ,take-end ,job-id)
                           ,(if threading?
                                `(type fixnum ,portion-id)
                                `(ignore ,portion-id)))
                  (let ((,line-var (aref ,line-array ,job-id))
                        (,result-var ,result-initform)
                        (,buffer-var (aref ,buffer-array ,job-id)))
                    (declare ,(if result-type
                                  `(type ,result-type ,result-var)
                                  `(ignorable ,result-var))
                             (type (simple-base-string ,(1- line-size)) ,line-var)
                             (ignorable ,line-var)
                             (type (simple-array ascii:ub-char (,buffer-size)) ,buffer-var))
                    (loop for ,offset-var fixnum from 0 below ,take-end by ,line-size
                          do ,@body)
                    ,(if threading?
                         `(cons (cons ,portion-id ,result-var)
                                ,job-id)
                         result-var))))
           (with-open-file (ins ,(spec-path spec) :direction :input
                                                  :element-type 'ascii:ub-char)
             ,(cond
                (threading?
                 `(let* ((result ,result-initform)
                         (lparallel:*kernel* (lparallel:make-kernel ,jobs))
                         (chan (lparallel:make-channel)))
                    ,(when result-type
                       `(declare (type ,result-type result)))
                    (unwind-protect
                         (let ((portion-count 0)
                               (next 1)
                               (ready-res (make-array 0 :fill-pointer 0 :adjustable t)))
                           (declare (type fixnum portion-count next)
                                    (type (vector (cons fixnum ,(or result-type t))) ready-res)
                                    (dynamic-extent ready-res))
                           (loop for i fixnum from 0 below ,jobs
                                 for bytes fixnum = (read-sequence (aref ,buffer-array i) ins)
                                 until (zerop bytes)
                                 do (lparallel:submit-task chan #'mapper bytes i
                                                           (incf portion-count)))
                           (when (= ,jobs portion-count)
                             (loop with more? = t
                                   while more?
                                   do (destructuring-bind (res . job-id)
                                          (lparallel:receive-result chan)
                                        (let ((bytes (read-sequence (aref ,buffer-array job-id) ins)))
                                          (if (zerop bytes)
                                              (setf more? nil)
                                              (lparallel:submit-task chan #'mapper bytes job-id
                                                                     (incf portion-count))))
                                        (vector-push-extend res ready-res))
                                   #1=(loop for res = (find next ready-res :key #'car)
                                            while res
                                            do (locally
                                                   (declare (type (cons fixnum ,(or result-type t))
                                                                  res))
                                                 (setf result (,reduce-fn result (cdr res))
                                                       ready-res (delete next ready-res
                                                                         :key #'car :test #'=)))
                                               (incf next)))
                             (lparallel:do-fast-receives (res chan
                                                              (min portion-count (1- ,jobs)))
                               (vector-push-extend (car res) ready-res)
                               #1#)))
                      (lparallel:end-kernel))
                    result))
                (reduce-fn
                 `(let ((result ,result-initform))
                    ,(when result-type
                       `(declare (type ,result-type result)))
                    (loop for bytes fixnum = (read-sequence (aref ,buffer-array 0) ins)
                          until (zerop bytes)
                          do (setf result (,reduce-fn result (mapper bytes 0 0))))
                    result))
                (t `(loop for bytes fixnum = (read-sequence (aref ,buffer-array 0) ins)
                          until (zerop bytes)
                          do (mapper bytes 0 0))))))))))
