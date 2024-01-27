package paxos

import (
	"coms4113/hw5/pkg/base"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
func ToA2RejectP1() []func(s *base.State) bool {
	return []func(s *base.State) bool{
		func(s *base.State) bool {
			// Check if server s1 is in the Propose phase
			s1 := s.Nodes()["s1"].(*Server)
			return s1.proposer.Phase == "Propose"
		},
		func(s *base.State) bool {
			// Check if server s1 has moved to the Accept phase
			s1 := s.Nodes()["s1"].(*Server)
			return s1.proposer.Phase == "Accept"
		},
	}
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	return []func(s *base.State) bool{
		func(s *base.State) bool {
			// Check if server s3 is in the Propose phase
			s3 := s.Nodes()["s3"].(*Server)
			return s3.proposer.Phase == "Propose"
		},
		func(s *base.State) bool {
			// Check if server s3 has moved to the Accept phase
			s3 := s.Nodes()["s3"].(*Server)
			return s3.proposer.Phase == "Accept"
		},
		// func(s *base.State) bool {
		// 	// Check if server s3 has received all OKs for its Accept request
		// 	event := s.Event
		// 	if event.Action != base.Handle && event.Action != base.HandleDuplicate {
		// 		return false
		// 	}
		// 	m, ok := event.Instance.(*AcceptResponse)
		// 	return ok && m.Ok && m.To() == "s3"
		// },
		func(s *base.State) bool {
			// Check if server s3 has moved to the Decide phase
			s3 := s.Nodes()["s3"].(*Server)
			return s3.proposer.Phase == "Decide"
		},
		func(s *base.State) bool {
			// Check if server s3 has reached consensus
			s3 := s.Nodes()["s3"].(*Server)
			// Assuming that agreedValue being not nil indicates consensus
			return s3.agreedValue == "v3"
		},
	}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	return []func(s *base.State) bool{
		func(s *base.State) bool {
			// Check if server s1 is in the Propose phase
			s1 := s.Nodes()["s1"].(*Server)
			return s1.proposer.Phase == "Propose"
		},
		func(s *base.State) bool {
			// Check if server s1 has moved to the Accept phase
			s1 := s.Nodes()["s1"].(*Server)
			return s1.proposer.Phase == "Accept"
		},
		func(s *base.State) bool {
			// Check if server s2 and s3 have a higher numbered proposal than s1 and have not reached consensus
			s1 := s.Nodes()["s1"].(*Server)
			s2 := s.Nodes()["s2"].(*Server)
			s3 := s.Nodes()["s3"].(*Server)
			return s1.proposer.Phase == "Accept" &&
				s2.proposer.N > s1.proposer.N && s3.proposer.N > s1.proposer.N
		},
	}
}

func NotTerminate2() []func(s *base.State) bool {
	return []func(s *base.State) bool{
		func(s *base.State) bool {
			// Check if server s3 is in the Propose phase
			s3 := s.Nodes()["s3"].(*Server)
			return s3.proposer.Phase == "Propose"
		},
		func(s *base.State) bool {
			// Check if server s3 has moved to the Accept phase
			s3 := s.Nodes()["s3"].(*Server)
			return s3.proposer.Phase == "Accept"
		},
		func(s *base.State) bool {
			// Check if server s1 and s2 have a higher numbered proposal than s3 and have not reached consensus
			s3 := s.Nodes()["s3"].(*Server)
			s1 := s.Nodes()["s1"].(*Server)
			s2 := s.Nodes()["s2"].(*Server)
			return s3.proposer.Phase == "Accept" &&
				s1.proposer.N > s3.proposer.N && s2.proposer.N > s3.proposer.N
		},
	}
}

func NotTerminate3() []func(s *base.State) bool {
	return []func(s *base.State) bool{
		func(s *base.State) bool {
			// Check if server s1 is in the Propose phase again
			s1 := s.Nodes()["s1"].(*Server)
			return s1.proposer.Phase == "Propose"
		},
		func(s *base.State) bool {
			// Check if server s1 has moved to the Accept phase again
			s1 := s.Nodes()["s1"].(*Server)
			return s1.proposer.Phase == "Accept"
		},
		func(s *base.State) bool {
			// Check if server s2 and s3 have a higher numbered proposal than s1 again and have not reached consensus
			s1 := s.Nodes()["s1"].(*Server)
			s2 := s.Nodes()["s2"].(*Server)
			s3 := s.Nodes()["s3"].(*Server)
			return s1.proposer.Phase == "Accept" &&
				s2.proposer.N > s1.proposer.N && s3.proposer.N > s1.proposer.N
		},
	}
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	return []func(s *base.State) bool{
		func(s *base.State) bool {
			s1 := s.Nodes()["s1"].(*Server)
			// Check if server s1 is in the Propose phase
			return s1.proposer.Phase == "Propose"
		},
	}
}

func concurrentProposer2() []func(s *base.State) bool {
	return []func(s *base.State) bool{
		func(s *base.State) bool {
			s3 := s.Nodes()["s3"].(*Server)
			// Check if server s3 is in the Decide phase and all its Accept requests are accepted
			return s3.proposer.Phase == "Decide" && s3.proposer.ResponseCount == len(s.Nodes()) && s3.proposer.SuccessCount == len(s.Nodes())
		},
	}
}
