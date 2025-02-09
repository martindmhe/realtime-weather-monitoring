package main

import (
	"encoding/json"
	"log"
	"time"

	"fmt"
	"io"
	"net/http"

	"github.com/IBM/sarama"
)

type WeatherResponse struct {
	Current struct {
		Time               string  `json:"time"`
		Temperature2m      float64 `json:"temperature_2m"`
		RelativeHumidity2m float64 `json:"relative_humidity_2m"`
	} `json:"current"`
}

type WeatherData struct {
	Temperature float64   `json:"temperature"`
	Humidity    float64   `json:"humidity"`
	Timestamp   time.Time `json:"timestamp"`
}

func main() {
	// hardcoded delay to allow kafka to fully start
	time.Sleep(time.Second * 5)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Producer.Retry.Backoff = time.Second * 1

	// create admin client to ensure topic exists
	admin, err := sarama.NewClusterAdmin([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create admin client: %s", err)
	}
	defer admin.Close()

	// Create topic if it doesn't exist
	err = admin.CreateTopic("weather-data", &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	if err != nil && err != sarama.ErrTopicAlreadyExists {
		log.Printf("Error creating topic: %v", err)
	}

	// connect to local kafka server on 9092
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer producer.Close()

	for {
		weatherData, err := fetchWeatherData()
		if err != nil {
			log.Printf("Failed to fetch weather data: %s", err)
			continue
		}

		jsonData, err := json.Marshal(weatherData)
		if err != nil {
			log.Fatalf("Failed to marshal weather data: %s", err)
		}

		msg := &sarama.ProducerMessage{
			Topic: "weather-data",
			Value: sarama.ByteEncoder(jsonData),
		}

		_, _, err = producer.SendMessage(msg)
		if err != nil {
			log.Fatalf("Failed to send message: %s", err)
		}

		log.Printf("Message sent to Kafka")
		// fetch every 2 seconds 
		// time.Sleep(time.Second * 2)
	}
}

func fetchWeatherData() (*WeatherData, error) {
	// current toronto weather data
	var apiURL string = "https://api.open-meteo.com/v1/forecast?latitude=43.6532&longitude=79.3832&current=temperature_2m,relative_humidity_2m"

	// fmt.Println("fetched")

	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}

	var response WeatherResponse
	err = json.Unmarshal(body, &response)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	timestamp, err := time.Parse("2006-01-02T15:04", response.Current.Time)
	if err != nil {
		return nil, fmt.Errorf("error parsing time: %v", err)
	}

	weatherData := &WeatherData{
		Temperature: response.Current.Temperature2m,
		Humidity:    response.Current.RelativeHumidity2m,
		Timestamp:   timestamp,
	}

	return weatherData, nil
}
