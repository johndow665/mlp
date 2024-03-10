package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Пожалуйста, укажите путь к файлу в качестве аргумента.")
		os.Exit(1)
	}

	filePath := os.Args[1]

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	fmt.Println("Успешное подключение к MongoDB.")

	db := client.Database("pass")
	collection := db.Collection("pass")

	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("Файл %s не найден.", filePath)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0

	for scanner.Scan() {
		record := scanner.Text()
		if record != "" {
			_, err := collection.InsertOne(ctx, bson.M{"data": record})
			if err != nil {
				log.Printf("Ошибка при добавлении записи в базу данных: %v", err)
				continue
			}
			count++
			if count%10000 == 0 {
				fmt.Printf("Добавлено %d записей в базу данных.\n", count)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Всего добавлено %d записей в базу данных.\n", count)
}
