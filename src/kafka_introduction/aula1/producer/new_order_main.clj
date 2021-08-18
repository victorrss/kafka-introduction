(ns kafka-introduction.aula1.producer.new-order-main
  (:import
    (java.util Properties)
    (org.apache.kafka.clients.admin AdminClient NewTopic)
    (org.apache.kafka.clients.producer Callback KafkaProducer ProducerConfig ProducerRecord)
    (org.apache.kafka.common.errors TopicExistsException)))

(defn- cria-as-propriedades-da-conexao-kafka []
  "Define as configurações(Serializer e Server) e devolve uma instância de Properties"
  (doto (Properties.)
    (.putAll {ProducerConfig/KEY_SERIALIZER_CLASS_CONFIG   "org.apache.kafka.common.serialization.StringSerializer"
              ProducerConfig/VALUE_SERIALIZER_CLASS_CONFIG "org.apache.kafka.common.serialization.StringSerializer"
              ProducerConfig/BOOTSTRAP_SERVERS_CONFIG      "localhost:9092"})))

(defn- cria-topico! [topico particoes replicacoes config]
  "Cria o tópico, análogo ao comando bin/kafka-topics.sh --create --bootstrap-server HOST:PORTA --replication-factor 1 --partitions 1 --topic NOME_DO_TOPICO"
  (let [ac (AdminClient/create config)]
    (try
      (.createTopics ac [(NewTopic. topico particoes replicacoes)])
      ;Ignora o TopicExistsException que é lançado quando faz a execução e o tópico já existe
      ; A intenção dessa func. é somente criar o tópico
      (catch TopicExistsException e nil)
      (finally
        (.close ac)))))

(defn cria-produtor! [topico chave valor]
  "Cria o produtor, análogo ao comando bin/kafka-console-producer.sh --broker-list HOST:PORTA --topic NOME_DO_TOPICO"
  (let [props (cria-as-propriedades-da-conexao-kafka)
        print-ex (comp println (partial str "Falha ao enviar a mensagem: "))
        print-metadata #(printf "Enviado com sucesso para %s ::: partition %d | offset %d | timestamp %d\n"
                                (.topic %)
                                (.partition %)
                                (.offset %)
                                (.timestamp %))
        cria-registro-da-mensagem #(let [k chave
                                         v valor]
                                     (printf "Registro do produtor: %s\t%s\n" k v)
                                     (ProducerRecord. topico k v))]
    ; Inicia o produtor
    (with-open [produtor (KafkaProducer. props)]
      ; Cria o tópico se ele já não existir
      (cria-topico! topico 1 1 props)
      ; Criação do callback para pegar o resultado da chamada asíncrona(Future)
      (let [callback (reify Callback
                       (onCompletion [_ metadata ex]
                         (if ex
                           (print-ex ex)
                           (print-metadata metadata))))]
        ; Envia a mensagem e pega o future dela.
        (let [msg-future (.send produtor (cria-registro-da-mensagem) callback)]
          ; Descarrega o produtor
          (.flush produtor)
          ; Não faz nada até que a future seja resolvida
          (while (not (future-done? msg-future))
            (Thread/sleep 50))
          ; Tenta ler o sucesso ou erro da Future
          (try
            (let [metadata (deref msg-future)]
              (print-metadata metadata))
            (catch Exception e
              (print-ex e))))))))

; Faz a mágica
(cria-produtor! "STORE_NEW_ORDER", "CHAVE", "VALOR")