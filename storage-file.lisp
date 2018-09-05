;;;; storage-file.lisp

(in-package #:fdbq)

(defvar *buffer-size* (/ (expt 1024 2) 2)
  "Bytes per job.")

(defclass spec-file (spec)
  ((path :type pathname :accessor spec-path :initarg :path)))

(defmethod defspec (name (type (eql :file)) fields &key path)
  "Register file DB with NAME, list of FIELDS and file PATH."
  (let ((spec (make-instance 'spec-file :path (merge-pathnames path))))
    (defspec-fields spec fields)
    (setf (gethash name *dbs*) spec)))

(defmethod gen-select ((spec spec-file) field-list where print jobs)
  "Generate selection procedure for FIELD-LIST from DB with WHERE filter."
  `(lambda () (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
     ,(if (and print (= 1 jobs))
          (gen-do-lines spec 'line
                        `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                            ,(gen-print-selection spec field-list 'line
                                                  :buffer-var 'buffer
                                                  :offset-var 'offset)))
                        :buffer-var 'buffer :offset-var 'offset)
          `(let ((res ,(gen-do-lines spec 'line
                                     `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                                         ,(gen-list-selection spec field-list 'line 'result
                                                              :buffer-var 'buffer
                                                              :offset-var 'offset)))
                                     :buffer-var 'buffer :offset-var 'offset
                                     :reduce-fn 'append-vec :jobs jobs
                                     :result-var 'result
                                     :result-initform '(make-array 0 :fill-pointer t)
                                     :result-type `(vector (simple-array simple-base-string
                                                                         (,(length field-list)))))))
             ,(if print
                  (gen-print-select-results 'res (length field-list))
                  'res)))))

(defmethod gen-print-selection ((spec spec-file) fields line-var
                                &key buffer-var offset-var)
  "Unroll selected FIELDS' print statements.
BUFFER-VAR is symbol representing the db buffer.
OFFSET-VAR is symbol representing the current offset in the db buffer."
  (declare (ignore line-var))
  `(progn
     ,@(loop for field in fields ;collect print statements in list and splice them
             collect '(write-char #\|)
             collect `(loop for i fixnum from (+ ,offset-var
                                                 ,(field-offset field spec))
                              below (+ ,offset-var ,(+ (field-offset field spec)
                                                       (field-size field spec)))
                            do (write-char (code-char (aref ,buffer-var i)))))
     (format t "|~%")))

(defmethod gen-list-selection ((spec spec-file) fields line-var result
                               &key buffer-var offset-var)
  "Unroll selected FIELDS' gather statements.
BUFFER-VAR is symbol representing the db buffer.
OFFSET-VAR is symbol representing the current offset in the db buffer."
  (declare (ignore line-var))
  `(let ((res (make-array ,(length fields))))
     ,@(loop for field in fields
             for i fixnum from 0
             collect `(setf (aref res ,i)
                            (let ((field-str (make-string ,(field-size field spec)
                                                          :element-type 'base-char)))
                              (loop for i fixnum from 0 below ,(field-size field spec)
                                    for j fixnum from (+ ,offset-var ,(field-offset field spec))
                                    do (setf (aref field-str i) (code-char (aref ,buffer-var j))))
                              field-str)))
     (vector-push-extend res ,result)))

(defmethod gen-cnt ((spec spec-file) where jobs)
  "Generate count procedure over DB with WHERE filter over file."
  `(lambda () (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0)))
     ,(gen-do-lines spec 'line
                    `((when ,(or (gen-where where 'line spec 'buffer 'offset) t)
                        (incf result)))
                    :buffer-var 'buffer :offset-var 'offset
                    :reduce-fn '+ :jobs jobs
                    :result-var 'result :result-initform 0 :result-type 'fixnum)))

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
                    (declare (optimize (speed 3) (debug 0) (safety 0) (compilation-speed 0))
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
                              (setf ,take-count (floor (read-sequence ,buffer-var ,ins) ,line-size)
                                    ,portion-id (incf ,job-count)))
                           `(setf ,take-count (floor (read-sequence ,buffer-var ,ins)
                                                     ,line-size)))
                      (loop for ,offset-var fixnum from 0 by ,line-size
                            repeat ,take-count
                            do ,@body)
                      ,(if threading?
                           `(cons (cons ,portion-id ,result-var)
                                  (when (= ,take-count ,(/ buffer-size line-size))
                                    ,job-id))
                           `(values ,result-var (= ,take-count
                                                   ,(/ buffer-size line-size)))))))
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
                           (let ((ready-res nil)
                                 (next 1))
                             (declare (type list ready-res)
                                      (type fixnum next))
                             (loop with more? = t
                                   while more?
                                   do (destructuring-bind (res . job-id)
                                          (lparallel:receive-result chan)
                                        (if job-id
                                            (lparallel:submit-task chan #'mapper job-id)
                                            (setf more? nil))
                                        (push res ready-res)
                                        #1=(loop for res = (assoc next ready-res)
                                                 while res
                                                 do (locally
                                                        (declare (type (cons fixnum ,result-type)
                                                                       res))
                                                      (setf result (,reduce-fn result (cdr res))
                                                            ready-res (delete next ready-res
                                                                              :key #'car)))
                                                    (incf next))))
                             (lparallel:do-fast-receives (res chan (1- ,jobs))
                               (push (car res) ready-res)
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
