(ns leiningen.emr
  (:use clojure.tools.cli))

(defn has [n xs]
  (= n (count xs)))

(defn select-truthy-keys
  [m key-seq]
  (->> (select-keys m key-seq)
       (filter second)
       (into {})))

(defn add-error [m e-string]
  (update-in m [:_errors] conj e-string))

(defn print-errors
  [error-seq]
  (doseq [e error-seq]
    (println e)))

(defn just-one? [m & kwds]
  (let [entries (select-truthy-keys m kwds)]
    (cond (has 1 entries) m
          (has 0 entries)
          (add-error m (str "Please provide one of the following: " kwds))
          :else (add-error m (str "Only one of the following is allowed: "
                                  (keys entries))))))

(defmacro build-validator
  [& validators]
  `(fn [arg-map#]
     (-> arg-map# ~@validators)))

(defn size-valid?
  "This step checks that, if size exist in the arg map,
  they're accompanied by a valid size. If this passes, the function acts as
  identity, else an error is added to the map."
  [{:keys [size] :as m}]
  (if (nil? size)
    (add-error m "Please provide a valid cluster size.")
    m))

(defn bidprice-valid?
  "This step checks that, if `start` or `emr` AND 'bid' exist in the
  arg map, they're accompanied by a valid bid-price. If this passes,
  the function acts as identity, else an error is added to the map."
  [{:keys [bid] :as m}]
  (if (nil? bid)
    (add-error m "Please provide a valid bid price (must be a number).")
    m))

(defn type-valid?
  "This step checks that the user has given a valid type. If this passes,
  the function acts as identity, else an error is added to the map."
  [{:keys [type] :as m}]
  (if (or (nil? type)
          (not (or (= type "large")
                   (= type "high-memory")
                   (= type "cluster-compute"))))
    (add-error m "Please specify a valid type (large, high-memory, cluster-compute).")
    m))

(defn mappers-reducers-valid?
  [{:keys [mappers reducers] :as m}]
  (cond
   (and (contains? m :mappers) (nil? mappers))
   (add-error m "Invalid mappers option. Please specify a number.")
   (and (contains? m :reducers) (nil? reducers))
   (add-error m "Invalid reducers option. Please specify a number.")
   :else m))

(defn bid-or-ondemand?
  "Checks to make sure the user didn't specify --ondemand and a --bid."
  [{:keys [on-demand bid] :as m}]
  (if (and (not (nil? on-demand))
           (not (nil? bid)))
    (add-error m "You cannot specify --bid and --on-demand. Use on or the other.")
    m))

(def hadoop-validator
  (build-validator
;;   (just-one? :start :stop :emr :jobtracker-ip)
   (size-valid?)
   (type-valid?)
   (mappers-reducers-valid?)
   (bid-or-ondemand?)
   (bidprice-valid?)))

(defn parse-hadoop-args
  "Used to parse command line arguments.  Returns a vector containing
  a map of the parsed arguments, a vector of extra arguments that did
  not match known switches, and a documentation banner to provide
  usage instructions. Returns nil if an invalid switch was given. If a
  valid numeric option (size, bid price, mappers or reducers) is
  supplied, it is converted into a number. If an option is not
  specified, it is not put in the map (unless there's a default). If
  an invalid option is given, it is set to nil. Ex output: {:size
  25, :type large, :name dev}"
  [args]
  ;The spec vectors contain string aliases, documentation, a value,
  ;and an optional parse fn
  (try
    (cli args
         ["-n" "--name" "Name of cluster." :default "dev"]
         ["-t" "--type" "Type  cluster."]
         ["-s" "--size" "Size of cluster." :parse-fn #(try
                                                        (Long. %)
                                                        (catch Exception _
                                                          nil))]  
         ["-z" "--zone" "Specifies an availability zone."
          :default "us-east-1d"]
         ["-m" "--mappers" "Specifies number of mappers." :parse-fn #(try
                                                                       (Long. %)
                                                                       (catch Exception _
                                                                         nil))] 
         ["-r" "--reducers" "Specifies number of reducers." :parse-fn #(try
                                                                         (Long. %)
                                                                         (catch Exception _
                                                                           nil))]
         ["-b"  "--bid" "Specifies a bid price." :parse-fn #(try
                                                              (Float. %)
                                                              (catch Exception _
                                                                nil))]
         ["-d" "--on-demand" "Uses on demand-pricing for all nodes."]
         ;;         ["--jobtracker-ip" "Print jobtracker IP address?"]
         ;;         ["--start" "Starts the EMR job flow."]
         ;;         ["--emr" "Boots an EMR cluster."]
         ;;         ["--stop" "Kills a pallet cluster."]
         )
    (catch Exception e (do (println (.getMessage e))
                           nil))))

(defn emr
  "Generates a command for the ruby elastic-mapreduce client based
  upon the given command line arguments and executes it. We first
  parse the command line args, display help usage or errors for
  invalid switches (if there were any), then validate the arguments
  map and display error validations (if there were any). If it passes
  everything then we generate/execute the ruby script with the given
  args."
  [project & args]
  (when-let [arg-map (parse-hadoop-args args)] ;;no invalid swich
    (if (= "help" (first (second arg-map)))
      (println (last arg-map)) ;; print usage for 'help'
      (let [val-arg-map (hadoop-validator (first arg-map))]
        (if-let [e-seq (:_errors val-arg-map)]
          (print-errors e-seq) ;;if validation contains error
          (println "map after validation:  " val-arg-map) ;;no error
          )))))
