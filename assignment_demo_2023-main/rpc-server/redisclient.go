package main

import (
	"context"
	"encoding/json"
	"github.com/redis/go-redis/v9"
	"log"
)

type Message struct {
	Sender    string `json:"sender"`
	Content   string `json:"content"`
	Timestamp int64  `json:"timestamp"`
}

type RedisClient struct {
	client *redis.Client
}

// initialising redis
func IniRedisClient(address, password string, db int) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       db,
	})

	err := rdb.Ping(context.Background()).Err()
	if err != nil {
		return nil, err
	}

	return &RedisClient{client: rdb}, nil
}

func (rc *RedisClient) SaveMessage(message *Message) error {
	ctx := context.Background()

	data, err := json.Marshal(message)
	if err != nil {
		log.Printf("Failed to marshal message %+v: %v", message, err)
		return err
	}

	err = rc.client.HSet(ctx, "messages", data).Err()
	if err != nil {
		log.Printf("Failed to save message with ID: %v", err)
		return err
	}

	return nil
}

func (rc *RedisClient) GetMessages(sender string) ([]Message, error) {
	ctx := context.Background()

	//getting all messages
	messageData, err := rc.client.HGetAll(ctx, "messages").Result()
	if err != nil {
		log.Printf("Failed to get messages: %v", err)
		return nil, err
	}

	var messages []Message

	//filtering to get all messages from sender in the parameter
	for _, data := range messageData {
		var message Message
		err := json.Unmarshal([]byte(data), &message)
		if err != nil {
			log.Printf("Failed to unmarshal message: %v", err)
			continue
		}
		messages = append(messages, message)
	}
	return messages, nil
}
