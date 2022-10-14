package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func IsValidDomain(category string) bool {
	switch category {
	case
		"gcn.nasa.gov",
		"test.gcn.nasa.gov",
		"dev.gcn.nasa.gov":
		return true
	}
	return false
}

func NewConsumer(domain string, group_id string, client_id string, client_secret string, topics []string) (*kafka.Consumer, error) {

	// verify that server is a gcn server
	if !IsValidDomain(domain) {
		return nil, fmt.Errorf(fmt.Sprintf("Invalid domain: %s", domain))
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":                   fmt.Sprintf("kafka.%s", domain),
		"sasl.mechanisms":                     "OAUTHBEARER",
		"sasl.oauthbearer.method":             "oidc",
		"security.protocol":                   "sasl_ssl",
		"ssl.ca.location":                     "/etc/ssl/certs/ca-certificates.crt",
		"sasl.oauthbearer.client.id":          client_id,
		"sasl.oauthbearer.client.secret":      client_secret,
		"sasl.oauthbearer.token.endpoint.url": fmt.Sprintf("https://auth.%s/oauth2/token", domain),
		"group.id":                            group_id,
		"auto.offset.reset":                   "earliest",
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics(topics, nil)

	return c, nil
}

func get_ivorn(content string) (string, error) {
	var ivorn string
	if strings.Contains(content, "ivorn=\"") {
		ivorn = strings.Split(content, "ivorn=\"")[1]
	} else {
		return "", fmt.Errorf("no ivorn found in content 1")
	}
	if strings.Contains(ivorn, "\"") {
		ivorn = strings.Split(ivorn, "\"")[0]
	} else {
		return "", fmt.Errorf("no ivorn found in content 2")
	}
	if strings.Contains(content, "/") {
		ivorn = strings.Split(ivorn, "/")[len(strings.Split(ivorn, "/"))-1]
	} else {
		return "", fmt.Errorf("no ivorn found in content 3")
	}
	return ivorn, nil
}

func get_topic(topic string) string {
	topic_split := strings.Split(topic, ".")
	topic = topic_split[len(topic_split)-1]
	//remove all spaces and \n, \t from the topic
	topic = strings.ReplaceAll(topic, " ", "")
	topic = strings.ReplaceAll(topic, "\n", "")
	return topic
}

func save_to_disk(ivorn string, topic string, content string) {
	// create a directory for the topic if it does not exist
	if _, err := os.Stat(topic); os.IsNotExist(err) {
		os.Mkdir(topic, 0755)
	}
	// check if the file exists and if it does, do not overwrite it
	if _, err := os.Stat(fmt.Sprintf("%s/%s", topic, ivorn)); os.IsNotExist(err) {

		// create a file with the ivorn as name
		file, err := os.Create(fmt.Sprintf("%s/%s", topic, ivorn))
		if err != nil {
			panic(err)
		}
		defer file.Close()

		// write the content to the file
		_, err = file.WriteString(content)
		if err != nil {
			panic(err)
		}
	}
}

func read_config_from_file() (string, string, string, string, []string) {
	// read the config from a file
	_, err1 := os.Stat("gcn-kafka.conf")
	_, err2 := os.Stat("gcn-kafka.default.conf")

	if os.IsNotExist(err1) && os.IsNotExist(err2) {
		fmt.Println("Default config file does not exist.\nCreating default config file...")
		file, err := os.Create("gcn-kafka.default.conf")
		if err != nil {
			panic(err)
		}
		// write the default config to the file
		_, err = file.WriteString("domain = gcn.nasa.gov\nclient_id = \nclient_secret = \ngroup_id = \ntopics = \n")
		if err != nil {
			panic(err)
		}
		file.Close()
	}

	// check if a file named gcn-kafka.conf exists
	if _, err := os.Stat("gcn-kafka.conf"); os.IsNotExist(err) {
		fmt.Println("Config file does not exist.\nPlease create a copy of gcn-kafka.default.conf and rename it to gcn-kafka.conf. Then fill in the values.\nExiting...")
		os.Exit(1)
	}

	file, err := os.Open("gcn-kafka.conf")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// read the domain, group_id, client_id, client_secret and topics from the file
	var domain, group_id, client_id, client_secret, topics_str string
	var topics []string

	// read the domain
	_, err = fmt.Fscanf(file, "domain: %s", &domain)
	if err != nil {
		fmt.Println("Could not read domain from config file!\nExiting...")
		os.Exit(1)
	}
	// read the group_id
	_, err = fmt.Fscanf(file, "group_id: %s", &group_id)
	if err != nil {
		fmt.Println("Could not read group_id from config file!\nExiting...")
		os.Exit(1)
	}
	// read the client_id
	_, err = fmt.Fscanf(file, "client_id: %s", &client_id)
	if err != nil {
		fmt.Println("Could not read client_id from config file!\nExiting...")
		os.Exit(1)
	}
	// read the client_secret
	_, err = fmt.Fscanf(file, "client_secret: %s", &client_secret)
	if err != nil {
		fmt.Println("Could not read client_secret from config file!\nExiting...")
		os.Exit(1)
	}

	_, err = fmt.Fscanf(file, "topics:")
	if err != nil {
		fmt.Println("Could not read topics from config file!\nExiting...")
		os.Exit(1)
	}

	count := 0
	for count < 2 {
		// read the topics
		_, err = fmt.Fscanf(file, "- %s", &topics_str)
		if err != nil || topics_str == "" {
			count = count + 1
		} else {
			topics = append(topics, strings.ReplaceAll(topics_str, " ", ""))
		}
	}

	if len(topics) == 0 {
		fmt.Println("No topics found.\nExiting...")
		os.Exit(1)
	}

	return domain, group_id, client_id, client_secret, topics
}

func main() {

	// read the config from a file
	domain, group_id, client_id, client_secret, topics := read_config_from_file()

	// print the config
	fmt.Println("domain:", domain)
	fmt.Println("group_id:", group_id)
	fmt.Println("client_id:", client_id)
	fmt.Println("client_secret:", client_secret)
	fmt.Println("topics:")

	for _, element := range topics {
		fmt.Printf(" - %s\n", element)
	}
	fmt.Printf("\n")

	// create a new consumer
	c, err := NewConsumer(domain, group_id, client_id, client_secret, topics)
	if err != nil {
		panic(err)
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(1000)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				content := string(e.Value)

				ivorn, err := get_ivorn(content)
				if err != nil {
					fmt.Println(err)
					continue
				}

				topic := get_topic(*e.TopicPartition.Topic)
				fmt.Printf("ivorn: %s, topic: %s\n", ivorn, topic)

				save_to_disk(ivorn, topic, content)

			case kafka.Error:
				// Errors should generally be considered informational, the client will try to automatically recover.
				// But in this example we choose to terminate the application if all brokers are down.
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				// do nothing
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}
