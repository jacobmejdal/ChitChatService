package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"unicode/utf8"

	proto "chitchat/grpc"

	"google.golang.org/grpc"
)

type Participant struct {
	name   string
	stream proto.ChitChat_JoinServer
}

type server struct {
	proto.UnimplementedChitChatServer

	participants []*Participant
	lamportTime  int32
	mu           sync.Mutex
}

func (s *server) nextTimestamp() int32 {
	s.lamportTime++
	return s.lamportTime
}

func (s *server) Join(
	in *proto.JoinRequest,
	stream proto.ChitChat_JoinServer,
) error {

	participant := &Participant{
		name:   in.GetParticipantName(),
		stream: stream,
	}

	s.mu.Lock()

	s.participants = append(s.participants, participant)

	logicalTime := s.nextTimestamp()

	log.Printf(
		"[SERVER][JOIN] Client %s joined at logical time %d",
		participant.name,
		logicalTime,
	)

	for _, p := range s.participants {
		err := p.stream.Send(&proto.Broadcast{
			Message: fmt.Sprintf(
				"Participant %s joined Chit Chat at logical time %d",
				participant.name,
				logicalTime,
			),
			LogicalTime: logicalTime,
		})

		if err != nil {
			log.Printf(
				"[SERVER][ERROR] Could not notify %s: %v",
				p.name,
				err,
			)
		}
	}

	s.mu.Unlock()

	<-stream.Context().Done()

	s.mu.Lock()

	s.participants = removeParticipant(
		s.participants,
		participant,
	)

	s.mu.Unlock()

	log.Printf(
		"[SERVER][DISCONNECT] %s disconnected",
		participant.name,
	)

	return nil
}

func removeParticipant(
	list []*Participant,
	target *Participant,
) []*Participant {

	result := []*Participant{}

	for _, p := range list {
		if p != target {
			result = append(result, p)
		}
	}

	return result
}

func (s *server) Publish(
	ctx context.Context,
	in *proto.PublishRequest,
) (*proto.Empty, error) {

	if utf8.RuneCountInString(in.GetMessage()) > 128 {
		log.Printf(
			"[SERVER][REJECTED] Message from %s exceeds 128 characters",
			in.GetParticipantName(),
		)
		return nil, fmt.Errorf("message exceeds maximum length of 128 characters")
	}

	s.mu.Lock()
	logicalTime := s.nextTimestamp()

	log.Printf(
		"[SERVER][MESSAGE] %s: %s (logical time %d)",
		in.GetParticipantName(),
		in.GetMessage(),
		logicalTime,
	)

	for _, participant := range s.participants {

		err := participant.stream.Send(
			&proto.Broadcast{
				Message: fmt.Sprintf(
					"%s: %s",
					in.GetParticipantName(),
					in.GetMessage(),
				),
				LogicalTime: logicalTime,
			},
		)

		if err != nil {
			log.Printf(
				"[SERVER][ERROR] Could not send to %s: %v",
				participant.name,
				err,
			)
		} else {
			log.Printf(
				"[SERVER][DELIVERY] Message delivered to %s",
				participant.name,
			)
		}
	}

	s.mu.Unlock()

	return &proto.Empty{}, nil
}

func (s *server) Leave(
	ctx context.Context,
	in *proto.LeaveRequest,
) (*proto.Empty, error) {

	s.mu.Lock()

	for i, participant := range s.participants {

		if participant.name == in.GetParticipantName() {

			s.participants = append(
				s.participants[:i],
				s.participants[i+1:]...,
			)

			break
		}
	}

	logicalTime := s.nextTimestamp()

	log.Printf(
		"[SERVER][LEAVE] %s left (logical time %d)",
		in.GetParticipantName(),
		logicalTime,
	)

	for _, participant := range s.participants {

		err := participant.stream.Send(
			&proto.Broadcast{
				Message: fmt.Sprintf(
					"Participant %s left Chit Chat at logical time %d",
					in.GetParticipantName(),
					logicalTime,
				),
				LogicalTime: logicalTime,
			},
		)

		if err != nil {
			log.Printf(
				"[SERVER][ERROR] Could not send leave message to %s: %v",
				participant.name,
				err,
			)
		}
	}

	s.mu.Unlock()

	return &proto.Empty{}, nil
}

func main() {

	log.Println("[SERVER] Starting Chit Chat server...")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	proto.RegisterChitChatServer(
		grpcServer,
		&server{},
	)

	log.Println("[SERVER] Listening on :50051")

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf(
			"failed to serve: %v",
			err,
		)
	}
}
