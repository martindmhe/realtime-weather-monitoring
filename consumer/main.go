package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
	"github.com/fatih/color"

)

type WeatherData struct {
	Temperature float64   `json:"temperature"`
	Humidity    float64   `json:"humidity"`
	Timestamp   time.Time `json:"timestamp"`
}

func main() {
	config := sarama.NewConfig()

	// fmt.Println("starting")

	// consmer config, rebalance stratefy is round robin for if we have multiple consumers
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

	// create a consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating consumer: %v", err)
	}
	defer consumer.Close()

	// subscribe to weather-data topic, offsetNewest means start from the latest message
	partitionConsumer, err := consumer.ConsumePartition("weather-data", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer partitionConsumer.Close()

	// create a channel for os signals
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// fmt.Println("consumer started")

	// purely for styling lol
	tempColor := color.New(color.FgRed).SprintfFunc()
	humidityColor := color.New(color.FgBlue).SprintfFunc()

	for {
		// wait for new message from partition consumer channel or shutdown signal
		select {
		// if message received is from the partition consumer
		case msg := <-partitionConsumer.Messages():
			// initialize weather data object with the weather data schema
			var weatherData WeatherData
			if err := json.Unmarshal(msg.Value, &weatherData); err != nil {
				log.Printf("Failed to unmarshal weather data: %s", err)
				continue
			}

			// Display weather data with colors
			fmt.Println("🌡️  Real-time Toronto Weather Monitor")
			fmt.Println("============================")
			fmt.Printf("\nTemperature: %s°C\n", tempColor("%.2f", weatherData.Temperature))
			fmt.Printf("Humidity: %s%%\n", humidityColor("%.2f", weatherData.Humidity))
			fmt.Printf("\nLast Weather Update: %s\n", weatherData.Timestamp.Format("15:04:05"))
            fmt.Printf("\nCurrent Time: %s\n", time.Now().Format("15:04:05"))
            fmt.Println("============================")

		case <-signals:
			// if message received is from signals channel:
			return

		}
	}

}
