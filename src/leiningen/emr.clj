(ns leiningen.emr
  (:use clojure.tools.cli
        clojure.xml
        [clojure.java.shell :only (sh)]
        [clojure.string :only (join)]))

;;HELPER FUNCTIONS
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

(def config-hadoop-bsa
  "s3://elasticmapreduce/bootstrap-actions/configure-hadoop")


;;SENSIBLE DEFAULTS
(def default-bid-price
  {"large" 0.32
   "cluster-compute" 1.30
   "high-memory" 1.80})

(defn calc-maps-reds
  "Given the type, it will return a map of the number of map tasks and
  reduce tasks using sensible defaults."
  [type mappers reducers]
  (cond
   (= type "large")             {:map-tasks (or mappers 4)
                                 :reduce-tasks (or reducers 2)}
   (= type "high-memory")       {:map-tasks (or mappers 30)
                                 :reduce-tasks (or reducers 24)}
   (= type "cluster-compute")   {:map-tasks (or mappers 22)
                                 :reduce-tasks (or reducers 16)}))

(defn convert-type
  [type]
  (cond
   (= type "large") "m1.large"
   (= type "high-memory") "m2.4xlarge"
   (= type "cluster-compute") "cc1.4xlarge"))

(defn base-props
  [map-tasks reduce-tasks node-count]
  {:mapred.reduce.tasks (int (* reduce-tasks node-count))
   :mapred.tasktracker.map.tasks.maximum map-tasks
   :mapred.tasktracker.reduce.tasks.maximum reduce-tasks})

(defn parse-config-hadoop
  "Takes in a config map of hadoop props (config file settings) and
  returns a string of hadoop properties for use by the ruby
  elastic-mapreduce script. You do not have to specify a site config
  file."
  [conf-map config-file]
  (let [arg-str (->> (map (fn [[k v]]
                            (format "-s,%s=%s" (name k) v)) conf-map)
                     (join ","))]
    (if config-file
      (format " --args --site-config-file,%s,%s" config-file arg-str)
      (format " --args \"%s\"" arg-str))))

(defn scriptify-bootstrap-xml
  "Takes in an xml file of bootstrap actions and generates a string of
  options for the elastic-mapreduce script."
  [file map-tasks reduce-tasks size]
  (clojure.string/trim
   (apply str
          (for [{:keys [attrs content]}
                (:content (parse file))]
            (str " --bootstrap-action " (:script attrs)
                 (let [args (map :content content)]
                   (if (= config-hadoop-bsa (:script attrs))
                     (parse-config-hadoop (base-props map-tasks reduce-tasks size)
                                          (:site-config-file attrs))
                     (when (seq args)
                       (apply str " --args "
                              (interpose ", "
                                         (apply concat args)))))))))))

;;SCRIPT GENERATION
(defn boot-emr!
  [{:keys [name type size zone mappers reducers bid on-demand bootstrap ami-version]
    :or {ami-version "2.0.5"}
    :as m}]
  (let [{:keys [map-tasks reduce-tasks]}
        (calc-maps-reds type mappers reducers)
        hw-id (convert-type type)
        bid-str (if (nil? bid)
                  (if (nil? on-demand)
                    (str "--bid-price " (default-bid-price type))
                    "")
                  (str " --bid-price " bid))
        bootstrap-str   (if bootstrap
                          (scriptify-bootstrap-xml bootstrap map-tasks reduce-tasks size)
                          "")]
    (->> #"\s"
         (clojure.string/split (format "elastic-mapreduce --create --alive --name %s --availability-zone %s --ami-version %s --instance-group master --instance-type %s --instance-count 1 --instance-group core --instance-type %s --instance-count %d --enable-debugging %s %s" name, zone, ami-version, hw-id, hw-id, size, bid-str, bootstrap-str))
         (apply sh)
         (:out)
         (println))))

;;VALIDATORS
(defn size-valid?
  "This step checks that, if size exist in the arg map,
  they're accompanied by a valid size. If this passes, the function acts as
  identity, else an error is added to the map."
  [{:keys [size] :as m}]
  (if (or (nil? size) (= size -1))
    (add-error m "Please provide a valid cluster size.")
    m))

(defn bidprice-valid?
  "This step checks that, if `start` or `emr` AND 'bid' exist in the
  arg map, they're accompanied by a valid bid-price. If this passes,
  the function acts as identity, else an error is added to the map."
  [{:keys [bid] :as m}]
  (if (and (contains? m :bid) (= bid -1))
    (add-error m "Please provide a valid bid price (must be a number).")
    m))

(defn type-valid?
  "This step checks that the user has given a valid type. If this passes,
  the function acts as identity, else an error is added to the map."
  [{:keys [type] :as m}]
  (if (or (nil? type)
          (= type -1)
          (not (or (= type "large")
                   (= type "high-memory")
                   (= type "cluster-compute"))))
    (add-error m "Please specify a valid type (large, high-memory, cluster-compute).")
    m))

(defn mappers-reducers-valid?
  [{:keys [mappers reducers bootstrap] :as m}]
  (cond
   (and (contains? m :mappers) (= mappers -1))
   (add-error m "Invalid mappers option. Please specify a number.")
   (and (contains? m :reducers) (= reducers -1))
   (add-error m "Invalid reducers option. Please specify a number.")
   (and (or mappers reducers) (nil? bootstrap))
   (add-error m
              "You must specify a bootstrap config file with the configure-hadoop bootstrap action to use --mappers or --reducers.")
   :else m))

(defn bid-or-ondemand?
  "Checks to make sure the user didn't specify --ondemand and a --bid."
  [{:keys [on-demand bid] :as m}]
  (if (and (seq on-demand)
           (seq bid))
    (add-error m "You cannot specify --bid and --on-demand. Use one or the other.")
    m))

(defn bootstrap-config-valid?
  "Checks to make sure the specified bootstrap config file exists and can be parsed."
  [{:keys [bootstrap] :as m}]
  (if (and (contains? m :bootstrap) (nil? bootstrap)) ;;user typed -bs
    ;;but didnt give a file
    (add-error m "Specify a valid bootstrap config file.")
    (if (nil? bootstrap)
      m ;;no bs config file specified, thats ok
      (try (when (parse bootstrap) m) ;;check to see if the file can
           ;;be parsed
           (catch Exception e (add-error m (.getMessage e)))))))

(def hadoop-validator
  (build-validator
   ;;   (just-one? :start :stop :emr :jobtracker-ip)
   (size-valid?)
   (type-valid?)
   (mappers-reducers-valid?)
   (bid-or-ondemand?)
   (bidprice-valid?)
   (bootstrap-config-valid?)))

(defn str-to-long
  [s]
  (try
    (Long. s)
    (catch Exception _
      -1)))

(defn str-to-float
  [s]
  (try
    (Float. s)
    (catch Exception _
      -1)))

;;PARSING CLI ARGS
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
         ["-t" "--type" "Type  cluster." :default "high-memory"]
         ["-s" "--size" "Size of cluster." :parse-fn str-to-long]
         ["-z" "--zone" "Specifies an availability zone."
          :default "us-east-1d"]
         ["-m" "--mappers" "Specifies number of mappers." :parse-fn str-to-long]
         ["-r" "--reducers" "Specifies number of reducers." :parse-fn str-to-long]
         ["-b"  "--bid" "Specifies a bid price." :parse-fn str-to-float]
         ["-d" "--on-demand" "Uses on demand-pricing for all nodes."]
         ["-bs" "--bootstrap" "Bootstrap config file location."]
         ;;         ["--jobtracker-ip" "Print jobtracker IP address?"]
         ;;         ["--start" "Starts the EMR job flow."]
         ;;         ["--stop" "Stops an EMR job flow."]
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
          (boot-emr! val-arg-map))))))
