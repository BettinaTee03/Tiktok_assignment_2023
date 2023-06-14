package main

import (
	"context"
	"log"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {

	message := &Message{
		Sender:    req.Message.GetSender(),
		Content:   req.Message.GetText(),
		Timestamp: time.Now().Unix(),
	}

	//saving the message
	err := s.RedisClient.SaveMessage(*message)
	if err != nil {
		log.Printf("Failed to save message: %v", err)
		return nil, err
	}
	resp := rpc.NewSendResponse()
	resp.Code, resp.Msg = 0, "success"
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {

	messages, err := s.RedisClient.GetMessages(req.Sender)
	if err != nil {
		log.Printf("Failed to retrieve messages: %v", err)
		return nil, err
	}

	var responseMessages []*rpc.Message
	for _, message := range messages {
		responseMessage := &rpc.Message{
			Sender:    message.Sender,
			Content:   message.Content,
			Timestamp: message.Timestamp,
		}
		responseMessages = append(responseMessages, responseMessage)
	}

	resp := rpc.NewPullResponse()
	resp.Code, resp.Msg = 0, "success"
	return resp, nil
}
