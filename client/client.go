package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	proto "chitchat/grpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)

	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	defer conn.Close()

	client := proto.NewChitChatClient(conn)

	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Enter username: ")

	username, err := reader.ReadString('\n')
	if err != nil {
		log.Fatal(err)
	}

	username = strings.TrimSpace(username)

	// Join chat
	stream, err := client.Join(
		context.Background(),
		&proto.JoinRequest{
			ParticipantName: username,
		},
	)

	if err != nil {
		log.Fatalf("Failed to join chat: %v", err)
	}

	fmt.Println("Connected to Chit Chat")
	fmt.Println("Type messages and press Enter")
	fmt.Println("Type /leave to exit")

	log.Printf("[CLIENT][CONNECT] %s connected to Chit Chat", username)

	// Listen for broadcasts
	go func() {

		for {

			broadcast, err := stream.Recv()

			if err == io.EOF {
				return
			}

			if err != nil {
				log.Printf("Receive error: %v", err)
				return
			}

			fmt.Printf(
				"\n[%d] %s\n",
				broadcast.GetLogicalTime(),
				broadcast.GetMessage(),
			)

			log.Printf(
				"[CLIENT][RECEIVED] %s (logical time %d)",
				broadcast.GetMessage(),
				broadcast.GetLogicalTime(),
			)
		}
	}()

	// Read user input
	for {

		text, err := reader.ReadString('\n')
		if err != nil {
			log.Fatal(err)
		}

		text = strings.TrimSpace(text)

		if text == "" {
			continue
		}

		if text == "/leave" {

			_, err := client.Leave(
				context.Background(),
				&proto.LeaveRequest{
					ParticipantName: username,
				},
			)

			if err != nil {
				log.Printf("Leave error: %v", err)
			}

			fmt.Println("Leaving chat...")
			log.Printf("[CLIENT][DISCONNECT] %s disconnected", username)
			return
		}

		_, err = client.Publish(
			context.Background(),
			&proto.PublishRequest{
				ParticipantName: username,
				Message:         text,
			},
		)

		if err != nil {
			log.Printf("Publish error: %v", err)
		}
	}
}
